defmodule Miruvor.LogEntry do
  @moduledoc """
  The log entry for Miruvor Raft.
  """
  alias __MODULE__
  @enforce_keys [:index, :term]
  defstruct term: 0, index: 0, operation: nil, requester: nil, arguments: nil

  @doc """
  Create an empty log entry.
  """
  @spec empty() :: %LogEntry{index: 0, term: 0}
  def empty() do
    %LogEntry{index: 0, term: 0}
  end

  @doc """
  Return a nop entry for the given index
  """
  @spec nop(integer(), integer(), atom()) :: %LogEntry{index: integer(), term: integer(), operation: :nop, requester: atom()}
  def nop(index, term, requester) do
    %LogEntry{index: index, term: term, operation: :nop, requester: requester}
  end

  @doc """
  Return a log entry for a get operation.
  """
  @spec get(integer(), integer(), atom(), binary()) :: %LogEntry{index: integer(), term: integer(), operation: :get, requester: atom(), arguments: binary()}
  def get(index, term, requester, key) do
    %LogEntry{index: index, term: term, operation: :get, requester: requester, arguments: key}
  end

  @doc """
  Return a log entry for a put operation.
  """
  @spec put(integer(), integer(), atom(), binary(), any()) :: %LogEntry{index: integer(), term: integer(), operation: :put, requester: atom(), arguments: {binary(), any()}}
  def put(index, term, requester, key, value) do
    %LogEntry{index: index, term: term, operation: :put, requester: requester, arguments: {key, value}}
  end

  @doc """
  Return a log entry for a delete operation.
  """
  @spec delete(integer(), integer(), atom(), binary()) :: %LogEntry{index: integer(), term: integer(), operation: :delete, requester: atom(), arguments: binary()}
  def delete(index, term, requester, key) do
    %LogEntry{index: index, term: term, operation: :delete, requester: requester, arguments: key}
  end

end

defmodule Miruvor.AppendEntryRequest do
  @moduledoc """
  The append entry request for Miruvor Raft.
  """
  alias __MODULE__
  @enforce_keys [:term, :leader_id, :prev_log_index, :prev_log_term, :leader_commit_index]
  defstruct term: nil, leader_id: nil, prev_log_index: nil, prev_log_term: nil, entries: nil, leader_commit_index: nil

  @doc """
  Create an append entry request.
  """

  @spec new(integer(), atom(), integer(), integer(), [Miruvor.LogEntry.t()], integer()) :: %AppendEntryRequest{term: integer(), leader_id: atom(), prev_log_index: integer(), prev_log_term: integer(), entries: [Miruvor.LogEntry.t()], leader_commit_index: integer()}
  def new(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit_index) do
    %AppendEntryRequest{term: term, leader_id: leader_id, prev_log_index: prev_log_index, prev_log_term: prev_log_term, entries: entries, leader_commit_index: leader_commit_index}
  end

end

defmodule Miruvor.AppendEntryResponse do
  @moduledoc """
  The append entry response for Miruvor Raft.
  """
  alias __MODULE__
  @enforce_keys [:term, :log_index, :success]
  defstruct term: nil, log_index: nil, success: nil

  @doc """
  Create an append entry response.
  """
  @spec new(integer(), integer(), boolean()) :: %AppendEntryResponse{term: integer(), log_index: integer(), success: boolean()}
  def new(term, log_index, success) do
    %AppendEntryResponse{term: term, log_index: log_index, success: success}
  end
end

defmodule Miruvor.RequestVoteRequest do
  @moduledoc """
  The request vote request for Miruvor Raft.
  """
  alias __MODULE__
  @enforce_keys [:term, :candidate_id, :last_log_index, :last_log_term]
  defstruct term: nil, candidate_id: nil, last_log_index: nil, last_log_term: nil

  @doc """
  Create a request vote request.
  """
  @spec new(integer(), atom(), integer(), integer()) :: %RequestVoteRequest{term: integer(), candidate_id: atom(), last_log_index: integer(), last_log_term: integer()}
  def new(term, candidate_id, last_log_index, last_log_term) do
    %RequestVoteRequest{term: term, candidate_id: candidate_id, last_log_index: last_log_index, last_log_term: last_log_term}
  end
end

defmodule Miruvor.RequestVoteResponse do
  @moduledoc """
  The request vote response for Miruvor Raft.
  """
  alias __MODULE__
  @enforce_keys [:term, :vote_granted]
  defstruct term: nil, vote_granted: nil

  @doc """
  Create a request vote response.
  """
  @spec new(integer(), boolean()) :: %RequestVoteResponse{term: integer(), vote_granted: boolean()}
  def new(term, vote_granted) do
    %RequestVoteResponse{term: term, vote_granted: vote_granted}
  end
end
