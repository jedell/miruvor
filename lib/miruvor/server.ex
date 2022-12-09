defmodule Miruvor.Server do
  use GenStateMachine, callback_mode: :state_functions
  alias Miruvor.RaftUtils
  require Logger

  @moduledoc """
  The server for Miruvor.
  """

  def start_link(opt) do
    Logger.info("Starting Raft")
    [{:node, node, :is_leader, leader}] = opt
    GenStateMachine.start_link(__MODULE__, [node: node, is_leader: leader], name: __MODULE__)
  end

  def init([node: _node, is_leader: false] = args) do
    Logger.info("Initializing Raft: #{inspect(args)}")
    Logger.info("Leader: false")
    state = RaftUtils.make_follower(RaftUtils.new_config(Node.list(), nil, 2000, 3000, 1000))
    {:ok, :follower, state}
  end

  def init([node: _node, is_leader: true] = args) do
    Logger.info("Initializing Raft: #{inspect(args)}")
    Logger.info("Leader: true")
    state = RaftUtils.make_leader(RaftUtils.new_config(Node.list(), nil, 2000, 3000, 1000))
    {:ok, :leader, state}
  end

  def read(key) do
    Logger.info("here first")
    GenStateMachine.call(__MODULE__, {:read, key})
  end

  def write(key, value) do
    GenStateMachine.call(__MODULE__, {:write, key, value})
  end

  def follower({:call, from}, {:read, key}, state) do
    Logger.info("Follower: Read #{key}")
    # redirect to leader
    reply = {:redirect, state.leader}
    {:keep_state_and_data, [{:reply, from, {:ok, "test"}}]}
  end

  def leader({:call, from}, {:read, key}, state) do
    Logger.info("Leader: Read #{key}")
    # redirect to leader
    # reply = {:redirect, state.leader}
    {:keep_state_and_data, [{:reply, from, {:ok, "test"}}]}
  end

end
