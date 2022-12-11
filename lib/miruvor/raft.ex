defmodule Miruvor.Raft do
  @moduledoc """
  The Consensus #{inspect(Node.self())} for Miruvor.
  Related work:
  - https://raft.github.io/raft.pdf
  - http://staff.um.edu.mt/afra1/papers/Erlang21.pdf
  - https://github.com/brianbinbin/Raf/blob/master/lib/raf/server.ex
  """
  use GenStateMachine, callback_mode: :state_functions
  require Logger
  alias Miruvor.Raft
  alias Miruvor.LogEntry
  alias Miruvor.RaftUtils
  import Plug.Conn, only: [send_resp: 3]

  defstruct(
    view: [],
    current_leader: nil,
    is_leader: false,
    current_term: 0,
    voted_for: nil,
    votes: [],
    state: nil,
    log: [],
    commit_index: nil,
    last_applied: nil,
    next_index: [],
    match_index: [],
    min_election_timeout: nil,
    max_election_timeout: nil,
    election_timer: nil,
    heartbeat_timeout: nil,
    heartbeat_timer: nil

    # state: :follower
  )

  def write(key, value) do
    Logger.info("Writing #{key} = #{value}")
    GenStateMachine.call(__MODULE__, {:write, {key, value}})
  end

  def read(key) do
    Logger.info("Reading #{key}")
    GenStateMachine.call(__MODULE__, {:read, key})
  end

  @spec start_link(any()) :: GenStateMachine.on_start()
  def start_link(opt) do
    Logger.info("Starting Raft")
    [{:node, node, :is_leader, leader}] = opt
    GenStateMachine.start_link(__MODULE__, [node: node, is_leader: leader], name: __MODULE__)
  end

  def init([node: _node, is_leader: true] = opt) do
    Logger.info("Initializing Raft: #{inspect(opt)}")
    Logger.info("Leader: true")
    state = RaftUtils.make_leader(RaftUtils.new_config(Node.list(), nil, 2000, 3000, 1000))
    {:ok, :leader, become_leader(state)}
  end

  def init([node: _node, is_leader: false] = opt) do
    Logger.info("Initializing Raft: #{inspect(opt)}")
    Logger.info("Leader: false")
    state = RaftUtils.make_follower(RaftUtils.new_config(Node.list(), nil, 2000, 3000, 1000))
    {:ok, :follower, become_follower(state)}
  end

  # get raft state
  def state() do
    GenStateMachine.call(__MODULE__, :state)
  end

  def is_leader?() do
    GenStateMachine.call(__MODULE__, :is_leader)
  end

  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  def handle_call(:is_leader, _from, state) do
    {:reply, state.is_leader, state}
  end

  # FOLLOWER HANDLERS

  def follower(:info, msg, state) do
    case msg do
      :election_timeout ->
        Logger.info("Follower #{inspect(Node.self())} election timeout")
        # Handle msg, return reply and new state
        # Become candidate
        state = become_candidate(state)

        # Send request vote to all other nodes
        RaftUtils.broadcast(
          state,
          :request_vote_req,
          Miruvor.RequestVoteRequest.new(
            state.current_term,
            Node.self(),
            RaftUtils.get_last_log_index(state),
            RaftUtils.get_last_log_term(state)
          )
        )

        {:next_state, :candidate, state}

      true ->
        {:keep_state, state}
    end
  end

  # LEADER HANDLERS

  def leader(:info, msg, state) do
    case msg do
      :heartbeat ->
        Logger.info("Leader #{inspect(Node.self())} heartbeat timeout")
        # Handle msg, return reply and new state
        state = RaftUtils.reset_heartbeat_timer(state)

        # Empty append_entries_req to followers (heartbeat)
        state =
          RaftUtils.broadcast(
            state,
            :append_entries_req,
            Miruvor.AppendEntryRequest.new(
              state.current_term,
              Node.self(),
              RaftUtils.get_last_log_index(state),
              RaftUtils.get_last_log_term(state),
              [],
              state.commit_index
            )
          )

        {:keep_state, state}

      _ ->
        {:keep_state, state}
    end
  end

  # CANDIDATE HANDLERS

  def candidate(:info, :election_timeout, state) do
    Logger.info("Candidate #{inspect(Node.self())} election timeout")
    # Become candidate
    state = become_candidate(state)

    {:keep_state, state}
  end

  # FOLLOWER STATES

  def follower({sender, :append_entries_resp, msg}, state) do
    Logger.debug(
      "Follower #{inspect(Node.self())} received append_entries_resp: #{inspect(msg)} from #{inspect(sender)}"
    )

    # implement logic for handling append_entries_resp

    {:ok, state}
  end

  def follower(:cast, {sender, :append_entries_req, msg}, state) do
    Logger.debug(
      "Follower #{inspect(Node.self())} received append_entries_req: #{inspect(msg)} from #{inspect(sender)}"
    )

    %Miruvor.AppendEntryRequest{
      term: term,
      leader_id: leader_id,
      prev_log_index: prev_log_index,
      prev_log_term: prev_log_term,
      entries: entries,
      leader_commit_index: leader_commit_index
    } = msg

    # Handle msg, return reply and new state (need function Raft.do_raft(msg, state))
    if term < state.current_term do
      response =
        Miruvor.AppendEntryResponse.new(
          state.current_term,
          RaftUtils.get_last_log_index(state),
          false
        )

      RaftUtils.send_to(sender, :append_entries_resp, response)
      {:keep_state, state}
    else
      if RaftUtils.get_log_entry(state, prev_log_index) != :noentry &&
           RaftUtils.get_log_entry(state, prev_log_index).term != prev_log_term do
        response =
          Miruvor.AppendEntryResponse.new(
            state.current_term,
            RaftUtils.get_last_log_index(state),
            false
          )

        RaftUtils.send_to(sender, :append_entries_resp, response)
        {:keep_state, state}
      else
        state = RaftUtils.reset_election_timer(state)

        conflict = RaftUtils.get_conflict(state, entries)
        state = RaftUtils.set_view(state, conflict)
        state = RaftUtils.append_log_entries(state, entries)
        # Logger.warn("Log after append: #{inspect(state.log)}")

        {_, state} =
          if leader_commit_index > state.commit_index do
            case RaftUtils.can_respond?(state) do
              {true, entry} ->
                {{requester, {:ok, value}}, raft} = RaftUtils.commit_log_entry(state, entry)
                # Logger.warn("Sending response to requester...")
                # GenStateMachine.reply(requester, {:ok, value})
                {:ok, state}

              _ ->
                {:ok, state}
            end

            # RaftUtils.commit_log_index(state, RaftUtils.get_last_log_index(state))
            # {:ok, state}
          else
            {:ok, state}
          end

        state =
          if leader_commit_index > state.commit_index do
            %{state | commit_index: min(leader_commit_index, RaftUtils.get_last_log_index(state))}
          else
            state
          end

        response =
          Miruvor.AppendEntryResponse.new(
            state.current_term,
            RaftUtils.get_last_log_index(state),
            true
          )

        RaftUtils.send_to(sender, :append_entries_resp, response)
        {:keep_state, state}
      end

      # new_state = state
      # {:ok, new_state}
    end
  end

  def follower(:cast, {sender, :request_vote_req, msg}, state) do
    Logger.info(
      "Follower #{inspect(Node.self())} received request_vote: #{inspect(msg)} from #{inspect(sender)}"
    )

    %Miruvor.RequestVoteRequest{
      term: term,
      candidate_id: candidate_id,
      last_log_index: last_log_index,
      last_log_term: last_log_term
    } = msg

    cond do
      term < state.current_term ->
        response = Miruvor.RequestVoteResponse.new(state.current_term, false)
        RaftUtils.send_to(sender, :request_vote_resp, response)
        {:keep_state, state}

      RaftUtils.get_last_log_term(state) >= last_log_term &&
        RaftUtils.get_last_log_index(state) >= last_log_index &&
          state.voted_for in [nil, candidate_id] ->
        state = %{state | voted_for: candidate_id}
        state = RaftUtils.reset_election_timer(state)
        state = %{state | current_leader: candidate_id}
        state = %{state | current_term: term}
        response = Miruvor.RequestVoteResponse.new(state.current_term, true)
        RaftUtils.send_to(sender, :request_vote_resp, response)
        {:keep_state, state}

      true ->
        response = Miruvor.RequestVoteResponse.new(state.current_term, false)
        RaftUtils.send_to(sender, :request_vote_resp, response)
        {:keep_state, state}
    end
  end

  def follower({sender, :request_vote_resp, msg}, state) do
    Logger.info(
      "Follower #{inspect(Node.self())} received request_vote_resp: #{inspect(msg)} from #{inspect(sender)}"
    )

    {:ok, state}
  end

  def follower({:call, from}, {:read, item}, state) do
    Logger.info("Follower #{inspect(Node.self())} received get: #{inspect(item)}")
    resp = RaftUtils.redirect_to(state.current_leader, :read, item)
    {:keep_state_and_data, [{:reply, from, resp}]}
  end

  def follower({:call, from}, {:write, {key, item}}, state) do
    Logger.info("Follower #{inspect(Node.self())} received put: #{inspect(item)}")
    resp = RaftUtils.redirect_to(state.current_leader, :write, {key, item})
    {:keep_state_and_data, [{:reply, from, resp}]}
  end

  ## LEADER STATES

  def leader({:call, from}, {:read, item}, state) do
    Logger.info("Leader #{inspect(Node.self())} received get: #{inspect(item)}")

    entry =
      Miruvor.LogEntry.get(
        RaftUtils.get_last_log_index(state) + 1,
        state.current_term,
        from,
        item
      )

    Logger.info("Entry: #{inspect(entry)}")
    state = RaftUtils.append_log_entry(state, entry)

    state = RaftUtils.get_view(state)
    # broadcast to followers
    Logger.info("Broadcasting to followers: #{inspect(state.view)}")

    RaftUtils.broadcast(
      state,
      :append_entries_req,
      Miruvor.AppendEntryRequest.new(
        state.current_term,
        Node.self(),
        RaftUtils.get_last_log_index(state),
        RaftUtils.get_last_log_term(state),
        [entry],
        state.commit_index
      )
    )

    {:keep_state, state}
  end

  def leader({:call, from}, {:write, {key, item}}, state) do
    Logger.info("Leader #{inspect(Node.self())} received post: #{inspect(item)}")

    entry =
      Miruvor.LogEntry.put(
        RaftUtils.get_last_log_index(state) + 1,
        state.current_term,
        from,
        key,
        item
      )

    Logger.info("Entry: #{inspect(entry)}")
    state = RaftUtils.append_log_entry(state, entry)

    # broadcast to followers
    Logger.info("Broadcasting to followers: #{inspect(state.view)}")

    RaftUtils.broadcast(
      state,
      :append_entries_req,
      Miruvor.AppendEntryRequest.new(
        state.current_term,
        Node.self(),
        RaftUtils.get_last_log_index(state),
        RaftUtils.get_last_log_term(state),
        [entry],
        state.commit_index
      )
    )

    {:keep_state, state}
  end

  def leader(:cast, {sender, :append_entries_resp, msg}, state) do
    Logger.info("Leader #{inspect(Node.self())} received append_entries_resp: #{inspect(msg)}")

    %Miruvor.AppendEntryResponse{
      term: term,
      log_index: log_index,
      success: success
    } = msg

    if success do
      state = RaftUtils.set_next_index(state, sender, log_index + 1)
      state = RaftUtils.set_match_index(state, sender, log_index)

      values =
        state.match_index
        |> Enum.map(fn {_, v} -> v end)
        |> Enum.sort(&>=/2)

      state = RaftUtils.get_view(state)

      {commit_index, _} =
        values
        |> Enum.map(fn v -> {v, Enum.count(values, &(&1 >= v))} end)
        |> Enum.find(fn {_, count} -> count >= Enum.count(state.view) / 2 end)

      if commit_index > state.commit_index do
        # resp = RaftUtils.commit_log_index(state, commit_index)
        # Logger.warn("resp: #{inspect(resp)}")
        # {{requester, {:ok, value}}, state} = resp
        case RaftUtils.can_respond?(state) do
          {true, entry} ->
            {{requester, {:ok, value}}, raft} = RaftUtils.commit_log_entry(state, entry)
            Logger.warn("Sending response to requester...")
            GenStateMachine.reply(requester, {:ok, value})
            {:keep_state, state}

          {false, entry} ->
            if entry.operation == :put do
              Logger.info("Write request, can reply and commit on correct shard")
              Logger.warn("Sending response to requester...")
              GenStateMachine.reply(entry.requester, {:ok, RaftUtils.get_key_from_entry(entry)})
              {:keep_state, state}
            else
              Logger.info("Read request, must commit")
              {{requester, {:ok, value}}, raft} = RaftUtils.commit_log_entry(state, entry)
              Logger.warn("Sending response to requester...")
              GenStateMachine.reply(requester, {:ok, value})
              {:keep_state, state}
            end
        end

        state = %{state | commit_index: commit_index}

        # GenStateMachine.reply(requester, {:ok, value})

        {:keep_state, state}
      else
        {:keep_state, state}
      end
    else
      if term > state.current_term do
        # step down to follower
        state = RaftUtils.set_current_term(state, term)
        state = become_follower(state)
        {:next_state, :follower, state}
      else
        state = %{
          state
          | next_index: Map.update!(state.next_index, sender, &(&1 - 1))
        }

        {:keep_state, state}
      end
    end
  end

  def leader(:cast, {sender, :append_entries_req, msg}, state) do
    Logger.info("Leader #{inspect(Node.self())} received append_entries_req: #{inspect(msg)}")

    %Miruvor.AppendEntryRequest{
      term: term,
      leader_id: leader_id,
      prev_log_index: prev_log_index,
      prev_log_term: prev_log_term,
      entries: entries,
      leader_commit_index: leader_commit
    } = msg

    if term < state.current_term do
      Logger.info(
        "Leader #{inspect(Node.self())} received append_entries_req with term < current_term"
      )

      # TODO: check index matters
      RaftUtils.send_to_reply(
        sender,
        :append_entries_resp,
        Miruvor.AppendEntryResponse.new(
          state.current_term,
          RaftUtils.get_last_log_index(state),
          false
        )
      )

      {:keep_state, state}
    else
      state = become_follower(state)
      {:next_state, :follower, state}
    end
  end

  def leader(:cast, {sender, :request_vote_resp, msg}, state) do
    Logger.info("Leader #{inspect(Node.self())} received request_vote_resp: #{inspect(msg)}")

    {:keep_state, state}
  end

  def leader(msg, state) do
    Logger.info("Leader #{inspect(Node.self())} received msg: #{inspect(msg)}")
    # Handle msg, return reply and new state (need function Raft.do_raft(msg, state))
    new_state = state
    {:ok, new_state}
  end

  ## CANDIDATE STATES

  def candidate(:cast, {sender, :request_vote_resp, msg}, state) do
    Logger.info("Candidate #{inspect(Node.self())} received request_vote_resp: #{inspect(msg)}")

    %Miruvor.RequestVoteResponse{
      term: term,
      vote_granted: granted
    } = msg

    state =
      if granted do
        RaftUtils.add_vote(state, sender)
      else
        state
      end

    cond do
      term > state.current_term ->
        state = RaftUtils.set_current_term(state, term)
        state = become_follower(state)
        {:next_state, :follower, state}

      RaftUtils.has_majority?(state) ->
        state = become_leader(state)
        {:next_state, :leader, state}

      true ->
        {:keep_state, state}
    end
  end

  def candidate(:cast, {sender, :request_vote_req, msg}, state) do
    Logger.info("Candidate #{inspect(Node.self())} received request_vote_req: #{inspect(msg)}")

    %Miruvor.RequestVoteRequest{
      term: term,
      candidate_id: candidate_id,
      last_log_index: last_log_index,
      last_log_term: last_log_term
    } = msg

    cond do
      term > state.current_term ->
        state = RaftUtils.set_current_term(state, term)

        {state, response} =
          RaftUtils.handle_vote(state, term, candidate_id, last_log_index, last_log_term)

        RaftUtils.send_to(sender, :request_vote_resp, response)
        state = become_follower(state)
        {:next_state, :follower, state}

      true ->
        response = Miruvor.RequestVoteResponse.new(state.current_term, false)
        RaftUtils.send_to(sender, :request_vote_resp, response)
        {:keep_state, state}
    end
  end

  def candidate(:cast, {sender, :append_entries_req, msg}, state) do
    Logger.info("Candidate #{inspect(Node.self())} received append_entries_req: #{inspect(msg)}")

    %Miruvor.AppendEntryRequest{
      term: term,
      leader_id: leader_id,
      prev_log_index: prev_log_index,
      prev_log_term: prev_log_term,
      entries: entries,
      leader_commit_index: leader_commit
    } = msg

    if term > state.current_term do
      state = RaftUtils.set_current_term(state, term)
      state = become_follower(state)
      {:next_state, :follower, state}
    else
      {:keep_state, state}
    end
  end

  def become_leader(state) do
    Logger.debug("Node #{inspect(Node.self())} become_leader")
    state = RaftUtils.get_view(state)

    state = RaftUtils.make_leader(state)

    # Empty append_entries_req to followers (heartbeat)
    RaftUtils.broadcast(
      state,
      :append_entries_req,
      Miruvor.AppendEntryRequest.new(
        state.current_term,
        Node.self(),
        RaftUtils.get_last_log_index(state),
        RaftUtils.get_last_log_term(state),
        [],
        state.commit_index
      )
    )

    state = RaftUtils.reset_heartbeat_timer(state)

    state
  end

  def become_candidate(state) do
    Logger.debug(
      "Node #{inspect(Node.self())} become_candidate with new term #{state.current_term + 1}"
    )

    state = RaftUtils.get_view(state)
    state = RaftUtils.make_candidate(state)
    state = RaftUtils.increment_term(state)

    # send request vote to all other nodes
    RaftUtils.broadcast(
      state,
      :request_vote_req,
      Miruvor.RequestVoteRequest.new(
        state.current_term,
        Node.self(),
        RaftUtils.get_last_log_index(state),
        RaftUtils.get_last_log_term(state)
      )
    )

    # reset election timer
    state = RaftUtils.reset_election_timer(state)

    state
  end

  def become_follower(state) do
    Logger.debug("Node #{inspect(Node.self())} become_follower")
    state = RaftUtils.make_follower(state)
    state = RaftUtils.reset_election_timer(state)
    state
  end
end
