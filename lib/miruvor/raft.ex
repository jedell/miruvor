defmodule Miruvor.Raft do
  @moduledoc """
  The Consensus #{inspect(Node.self())} for Miruvor.
  """

  use GenServer
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

  @spec start_link(any()) :: GenServer.on_start()
  def start_link(opt) do
    Logger.info("Starting Raft")
    [{:node, node, :is_leader, leader}] = opt
    GenServer.start_link(__MODULE__, [node: node, is_leader: leader], name: __MODULE__)
  end

  @spec init(any) :: {:ok, any}
  def init([node: _node, is_leader: true] = opt) do
    Logger.info("Initializing Raft: #{inspect(opt)}")
    Logger.info("Leader: true")
    state = RaftUtils.make_leader(RaftUtils.new_config(Node.list(), nil, 2000, 3000, 1000))
    {:ok, become_leader(state)}
  end

  def init([node: _node, is_leader: false] = opt) do
    Logger.info("Initializing Raft: #{inspect(opt)}")
    Logger.info("Leader: false")
    state = RaftUtils.make_follower(RaftUtils.new_config(Node.list(), nil, 2000, 3000, 1000))
    {:ok, become_follower(state)}
  end

  def send(msg) do
    GenServer.call(__MODULE__, msg)
  end

  def append_entries(msg) do
    GenServer.call(__MODULE__, {:append_entries, msg})
  end

  # get raft state
  def state() do
    GenServer.call(__MODULE__, :state)
  end

  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  # FOLLOWER HANDLERS

  @spec handle_call(any, any, any) :: {:reply, any, any}
  def handle_call(msg, _from, %{state: :follower} = state) do
    # Handle msg, return reply and new state
    {:ok, new_state} = follower(msg, state)
    # {:reply, item, new_state}
    {:reply, :ok, new_state}
  end

  def handle_info(msg, %{state: :follower} = state) do
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

        {:noreply, state}

      true ->
        {:noreply, state}
    end
  end

  # LEADER HANDLERS

  def handle_call(msg, _from, %{state: :leader} = state) do
    # Handle msg, return reply and new state
    {:ok, new_state} = leader(msg, state)
    # {:reply, item, new_state}
    {:reply, :ok, new_state}
  end

  def handle_info(msg, %{state: :leader} = state) do
    case msg do
      :heartbeat ->
        Logger.info("Leader #{inspect(Node.self())} heartbeat timeout")
        # Handle msg, return reply and new state
        state = RaftUtils.reset_heartbeat_timer(state)

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

        {:noreply, state}

      _ ->
        {:noreply, state}
    end
  end

  # CANDIDATE HANDLERS

  def handle_call(msg, _from, %{state: :candidate} = state) do
    # Handle msg, return reply and new state
    {:ok, new_state} = candidate(msg, state)
    # {:reply, item, new_state}
    {:reply, :ok, new_state}
  end

  def handle_info(:election_timeout, %{state: :candidate} = state) do
    Logger.info("Candidate #{inspect(Node.self())} election timeout")
    # Become candidate
    state = become_candidate(state)

    {:noreply, state}
  end

  # FOLLOWER STATES

  def follower({sender, :append_entries_resp, msg}, state) do
    Logger.debug(
      "Follower #{inspect(Node.self())} received append_entries_resp: #{inspect(msg)} from #{inspect(sender)}"
    )

    # implement logic for handling append_entries_resp

    {:ok, state}
  end

  def follower({sender, :append_entries_req, msg}, state) do
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
      {:ok, state}
    else
      state = RaftUtils.reset_election_timer(state)
      state = RaftUtils.set_current_term(state, term)

      if !RaftUtils.consistent?(state, prev_log_index, prev_log_term) do
        last_index = RaftUtils.get_last_log_index(state)

        state =
          if last_index >= prev_log_index do
            # clear logs from prev_log_index to last index
            RaftUtils.clear_log_entries(state, prev_log_index, last_index)
          else
            state
          end

        response =
          Miruvor.AppendEntryResponse.new(
            state.current_term,
            RaftUtils.get_last_log_index(state),
            false
          )

        RaftUtils.send_to(sender, :append_entries_resp, response)
        {:ok, state}
      else
        state = RaftUtils.append_log_entries(state, entries)
        {_, state} = RaftUtils.commit_log_index(state, min(leader_commit_index, RaftUtils.get_last_log_index(state)))
        state = RaftUtils.update_leader(state, leader_id)

        response =
          Miruvor.AppendEntryResponse.new(
            state.current_term,
            RaftUtils.get_last_log_index(state),
            true
          )

        RaftUtils.send_to(sender, :append_entries_resp, response)
        {:ok, state}
      end

      # new_state = state
      # {:ok, new_state}
    end
  end

  def follower({sender, :request_vote_req, msg}, state) do
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
        {:ok, state}

      RaftUtils.get_last_log_term(state) >= last_log_term &&
        RaftUtils.get_last_log_index(state) >= last_log_index &&
          state.voted_for in [nil, candidate_id] ->
        state = %{state | voted_for: candidate_id}
        state = RaftUtils.reset_election_timer(state)
        state = %{state | current_leader: candidate_id}
        state = %{state | current_term: term}
        response = Miruvor.RequestVoteResponse.new(state.current_term, true)
        RaftUtils.send_to(sender, :request_vote_resp, response)
        {:ok, state}

      true ->
        response = Miruvor.RequestVoteResponse.new(state.current_term, false)
        RaftUtils.send_to(sender, :request_vote_resp, response)
        {:ok, state}
    end
  end

  def follower({sender, :request_vote_resp, msg}, state) do
    Logger.info(
      "Follower #{inspect(Node.self())} received request_vote_resp: #{inspect(msg)} from #{inspect(sender)}"
    )

    {:ok, state}
  end

  def follower({:get, {conn, item} = _msg}, state) do
    Logger.info("Follower #{inspect(Node.self())} received get: #{inspect(item)}")
    RaftUtils.send_to(state.current_leader, :get, {conn, item})
    {:ok, state}
  end

  def follower({:post, {conn, key, item} = _msg}, state) do
    Logger.info("Follower #{inspect(Node.self())} received put: #{inspect(item)}")
    RaftUtils.send_to(state.current_leader, :post, {conn, key, item})
    {:ok, state}
  end

  ## LEADER STATES

  def leader({:get, {conn, item} = _msg}, state) do
    Logger.info("Leader #{inspect(Node.self())} received get: #{inspect(item)}")

    entry =
      Miruvor.LogEntry.get(
        RaftUtils.get_last_log_index(state) + 1,
        state.current_term,
        conn,
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

    {:ok, state}
  end

  def leader({:post, {conn, key, item} = _msg}, state) do
    Logger.info("Leader #{inspect(Node.self())} received post: #{inspect(item)}")

    entry =
      Miruvor.LogEntry.put(
        RaftUtils.get_last_log_index(state) + 1,
        state.current_term,
        conn,
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

    {:ok, state}
  end

  def leader({sender, :append_entries_resp, msg}, state) do
    Logger.info("Leader #{inspect(Node.self())} received append_entries_resp: #{inspect(msg)}")

    %Miruvor.AppendEntryResponse{
      term: term,
      log_index: log_index,
      success: success
    } = msg

    if success do
      state = RaftUtils.set_next_index(state, sender, log_index + 1)
      state = RaftUtils.set_match_index(state, sender, log_index)

      state = RaftUtils.get_view(state)

      commit_index =
        state.commit_index
        |> Stream.iterate(&(&1 + 1))
        |> Enum.find_value(fn index ->
          ## including me
          count = Enum.count(state.match_index, fn {_, mi} -> mi >= index end) + 1
          count <= state.view |> length() |> div(2) && index
        end)
        |> (&(&1 - 1)).()

      if commit_index > state.commit_index do
        {{requester, {:ok, value}}, state} = RaftUtils.commit_log_index(state, commit_index)
        state = %{state | commit_index: commit_index}

        Logger.warn("Sending response to requester...")
        send_resp(requester, 200, value)

        {:ok, state}
      else
        {:ok, state}
      end
    else
      if term > state.current_term do
        # step down to follower
        state = RaftUtils.set_current_term(state, term)
        state = become_follower(state)
        {:ok, state}
      else
        state = %{
          state
          | next_index: Map.update!(state.next_index, sender, &(&1 - 1))
        }

        {:ok, state}
      end
    end
  end

  def leader(msg, state) do
    Logger.info("Leader #{inspect(Node.self())} received msg: #{inspect(msg)}")
    # Handle msg, return reply and new state (need function Raft.do_raft(msg, state))
    new_state = state
    {:ok, new_state}
  end

  ## CANDIDATE STATES

  def candidate({sender, :request_vote_resp, msg}, state) do
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
        {:ok, state}

      RaftUtils.has_majority?(state) ->
        state = become_leader(state)
        {:ok, state}

      true ->
        {:ok, state}
    end
  end

  def candidate({sender, :request_vote_req, msg}, state) do
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
        {:ok, state}

      true ->
        response = Miruvor.RequestVoteResponse.new(state.current_term, false)
        RaftUtils.send_to(sender, :request_vote_resp, response)
        {:ok, state}
    end
  end

  def candidate({sender, :append_entries_req, msg}, state) do
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
      {:ok, state}
    else
      {:ok, state}
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
