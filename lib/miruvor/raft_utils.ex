defmodule Miruvor.RaftUtils do
  require Logger

  @moduledoc """
  The utils for Miruvor Raft.
  """

  # UTILS

  @spec new_config([atom()], atom(), integer(), integer(), integer()) :: %Miruvor.Raft{}
  @doc """
  Create a new Raft configuration.
  """
  def new_config(
        view,
        leader,
        min_election_timeout,
        max_election_timeout,
        heartbeat_timeout
      ) do
    %Miruvor.Raft{
      view: view,
      current_leader: leader,
      min_election_timeout: min_election_timeout,
      max_election_timeout: max_election_timeout,
      heartbeat_timeout: heartbeat_timeout,
      current_term: 1,
      voted_for: nil,
      is_leader: false,
      log: [],
      commit_index: 0,
      last_applied: 0,
      next_index: [],
      match_index: []
    }
  end

  @spec commit_log_entry(%Miruvor.Raft{}, Miruvor.LogEntry.t()) ::
          {{atom(), {:ok, any()} | {:error, :not_found | any()}}, %Miruvor.Raft{}}
  @doc """
  Commit a log entry.
  """
  def commit_log_entry(raft, log_entry) do
    case log_entry do
      %Miruvor.LogEntry{operation: :nop} ->
        Logger.debug("Committing nop entry")
        {{log_entry.requester, {:ok, :ok}}, raft}

      %Miruvor.LogEntry{operation: :get, arguments: key} ->
        Logger.debug("Committing get entry")
        {:ok, value} = Miruvor.Storage.get(key)
        {{log_entry.requester, {:ok, value}}, raft}

      %Miruvor.LogEntry{operation: :put, arguments: {key, value}} ->
        Logger.debug("Committing put entry")
        {:ok, value} = Miruvor.Storage.put(key, value)
        {{log_entry.requester, {:ok, value}}, raft}

      %Miruvor.LogEntry{operation: :delete, arguments: key} ->
        Logger.debug("Committing delete entry")
        {:ok, value} = Miruvor.Storage.delete(key)
        {{log_entry.requester, {:ok, value}}, raft}
        {:ok, raft}

      nil ->
        Logger.debug("Heartbeat received")
        {:ok, raft}

      _ ->
        Logger.debug("Committing unknown entry")
        {:error, raft}
    end
  end

  @spec commit_log_index(%Miruvor.Raft{}, integer()) ::
          {:noentry | {atom(), {:ok, any()} | {:error, :not_found | any()}}, %Miruvor.Raft{}}
  @doc """
  Commit a log entry by index.
  """
  def commit_log_index(raft, index) do
    if length(raft.log) < index do
      {:noentry, raft}
    else
      correct_idx = length(raft.log) - index

      IO.puts(
        "Committing log entry at index #{index} #{length(raft.log)} (correct index #{correct_idx})"
      )

      commit_log_entry(raft, Enum.at(raft.log, correct_idx))
    end
  end

  def reset_election_timer(raft) do
    if raft.election_timer != nil do
      Process.cancel_timer(raft.election_timer)
    end

    timer =
      Process.send_after(
        self(),
        :election_timeout,
        :rand.uniform(raft.max_election_timeout - raft.min_election_timeout) +
          raft.min_election_timeout
      )

    %Miruvor.Raft{raft | election_timer: timer}
  end

  def reset_heartbeat_timer(raft) do
    if raft.heartbeat_timer != nil do
      Process.cancel_timer(raft.heartbeat_timer)
    end

    timer = Process.send_after(self(), :heartbeat, raft.heartbeat_timeout)
    %Miruvor.Raft{raft | heartbeat_timer: timer}
  end

  @spec append_log_entry(%Miruvor.Raft{}, %Miruvor.LogEntry{}) :: %Miruvor.Raft{}
  @doc """
  Append a log entry to the log.
  """
  def append_log_entry(raft, log_entry) do
    %Miruvor.Raft{log: log} = raft
    new_log = [log_entry] ++ log
    %Miruvor.Raft{raft | log: new_log}
  end

  @spec append_log_entries(%Miruvor.Raft{}, [Miruvor.LogEntry.t()]) :: %Miruvor.Raft{}
  @doc """
  Append a list of log entries to the log.
  """
  def append_log_entries(raft, log_entries) do
    %Miruvor.Raft{log: log} = raft
    new_log = log_entries ++ log
    %Miruvor.Raft{raft | log: new_log}
  end

  def truncate_log(raft, index) do
    if length(raft.log) < index do
      # Nothing to do
      raft
    else
      to_drop = length(raft.log) - index + 1
      %{raft | log: Enum.drop(raft.log, to_drop)}
    end
  end

  def get_log_entry(raft, index) do
    if index <= 0 || index > length(raft.log) do
      :noentry
    else
      correct_idx = length(raft.log) - index
      Enum.at(raft.log, correct_idx)
    end
  end

  @spec get_last_log_index(%Miruvor.Raft{}) :: integer()
  @doc """
  Get the last log index.
  """
  def get_last_log_index(raft) do
    Enum.at(raft.log, 0, Miruvor.LogEntry.empty()).index
  end

  @spec get_last_log_term(%Miruvor.Raft{}) :: integer()
  @doc """
  Get the last log term.
  """
  def get_last_log_term(raft) do
    Enum.at(raft.log, 0, Miruvor.LogEntry.empty()).term
  end

  def set_current_term(raft, term) do
    %{raft | current_term: term}
  end

  def increment_term(raft) do
    %{raft | current_term: raft.current_term + 1}
  end

  def get_conflict(raft, entries) do
    Enum.find_index(entries, fn entry ->
      case get_log_entry(raft, entry.index) do
        :noentry -> false
        log_entry -> log_entry.term != entry.term
      end
    end)
  end

  def set_match_index(raft, server_id, index) do
    match_index = raft.match_index
    match_index = Map.put(match_index, server_id, index)
    %{raft | match_index: match_index}
  end

  def set_next_index(raft, server_id, index) do
    next_index = raft.next_index
    next_index = Map.put(next_index, server_id, index)
    %{raft | next_index: next_index}
  end

  defp already_voted?(state, sender) do
    state.voted_for != sender
  end

  defp up_to_date?(can_last_log_term, can_last_log_index, state) do
    state_last_log_term = get_last_log_term(state)
    state_last_log_index = get_last_log_index(state)

    cond do
      can_last_log_term > state_last_log_term ->
        true

      can_last_log_term < state_last_log_term ->
        false

      can_last_log_index > state_last_log_index ->
        true

      can_last_log_index < state_last_log_index ->
        false

      true ->
        true
    end
  end

  def has_majority?(raft) do
    raft = get_view(raft)
    majority = length(raft.view) / 2
    Enum.count(raft.votes) >= majority
  end

  def decide_vote(raft, term, candidate_id, last_log_index, last_log_term) do
    # reset election timer

    new_term = max(raft.current_term, term)
    raft = %Miruvor.Raft{raft | current_term: new_term}

    granted =
      if term < raft.current_term do
        false
      else
        if already_voted?(raft, candidate_id) do
          false
        else
          up_to_date?(last_log_term, last_log_index, raft)
        end
      end

    if granted do
      raft = %Miruvor.Raft{raft | voted_for: candidate_id}
      {raft, granted}
    else
      {raft, granted}
    end
  end

  @spec add_vote(%Miruvor.Raft{}, any()) :: %Miruvor.Raft{}
  def add_vote(raft, server_id) do
    votes = raft.votes
    votes = votes ++ [server_id]
    %{raft | votes: votes}
  end

  def handle_vote(raft, term, candidate_id, last_log_index, last_log_term) do
    raft = reset_election_timer(raft)
    raft = set_current_term(raft, term)
    {raft, granted} = decide_vote(raft, term, candidate_id, last_log_index, last_log_term)

    response = %Miruvor.RequestVoteResponse{
      term: raft.current_term,
      vote_granted: granted
    }

    {raft, response}
  end

  def set_view(raft, conflict) do
    if conflict do
      truncate_log(raft, conflict)
    else
      raft
    end
  end

  @spec make_leader(%Miruvor.Raft{}) :: %Miruvor.Raft{
          is_leader: true,
          next_index: map(),
          match_index: map()
        }
  @doc """
  Make this node the leader.
  """
  def make_leader(raft) do
    log_index = get_last_log_index(raft)
    next_index = Enum.map(raft.view, fn node -> {node, log_index} end) |> Enum.into(%{})
    match_index = Enum.map(raft.view, fn node -> {node, 0} end) |> Enum.into(%{})

    %Miruvor.Raft{
      raft
      | is_leader: true,
        state: :leader,
        next_index: next_index,
        match_index: match_index,
        current_leader: Node.self()
    }
  end

  @spec make_follower(%Miruvor.Raft{}) :: %Miruvor.Raft{
          is_leader: false,
          next_index: map(),
          match_index: map()
        }
  @doc """
  Make this node the follower.
  """
  def make_follower(raft) do
    %Miruvor.Raft{
      raft
      | is_leader: false,
        current_leader: nil,
        voted_for: nil,
        votes: [],
        state: :follower,
        next_index: %{},
        match_index: %{}
    }
  end

  def make_candidate(raft) do
    %Miruvor.Raft{
      raft
      | is_leader: false,
        state: :candidate,
        current_leader: nil,
        voted_for: Node.self(),
        votes: [Node.self()]
    }
  end

  def update_leader(raft, leader) do
    %Miruvor.Raft{raft | current_leader: leader}
  end

  @doc """
  Get the view.
  """
  def get_view(raft) do
    view = Node.list()
    %Miruvor.Raft{raft | view: view}
  end

  @spec broadcast(%Miruvor.Raft{is_leader: true}, atom(), any()) :: :ok
  @doc """
  Broadcast a message to all nodes in the view except self.
  """
  def broadcast(raft, message, payload) do
    Logger.info("Broadcasting #{message} to #{inspect(raft.view)}")

    raft = get_view(raft)
    # :rpc.multicall(raft.view, Miruvor.Raft, :send, [{Node.self(), message, payload}])
    # for each node in view, send message
    Enum.each(raft.view, fn node ->
      if node != Node.self() do
        resp = GenStateMachine.cast({Miruvor.Raft, node}, {Node.self(), message, payload})
        # :rpc.cast(node, Miruvor.Raft, :send, [{Node.self(), message, payload}])
      end
    end)

    # Logger.info("Broadcast response: #{inspect(resp)}")
    raft
  end

  def send_to(node, message, payload) do
    Logger.info("Sending #{message} to #{node}")
    GenStateMachine.cast({Miruvor.Raft, node}, {Node.self(), message, payload})
    # :rpc.cast(node, Miruvor.Raft, :send, [{Node.self(), message, payload}])
  end

  def send_to_reply(node, message, payload) do
    Logger.info("Sending #{message} to #{node}")
    GenStateMachine.call({Miruvor.Raft, node}, {Node.self(), message, payload})
  end

  def redirect_to(leader, message, payload) do
    Logger.info("Redirecting #{message} to leader #{inspect(leader)}")
    GenStateMachine.call({Miruvor.Raft, leader}, {message, payload})
  end

end
