defmodule MiruvorWeb.Controller do
  use MiruvorWeb, :controller
  require Logger

  @doc """
  This is the default controller for the MiruvorWeb application.
  The client interacts with these endpoints to perform CRUD operations on the
  database.
  """

  def index(conn, _params) do
    send_resp(conn, 200, "Hello World!")
  end

  def get(conn, _params) do
    # TODO: Implement raft consensus with strong consistency
    # state = GenServer.call(Miruvor.Raft, :state)
    # Logger.info("State: #{inspect(state)}")
    # resp = :rpc.multicall(Node.list(), Miruvor.Raft, :send, [%{sender: Node.self(), operation: :get}])
    # do raft, send_resp in raft, probably send a log entry here
    resp = GenServer.call(Miruvor.Raft, {:get, {conn, "test"}})

    # should return correct value
    send_resp(conn, 200, inspect(resp))
  end

  def post(conn, _params) do
    resp = GenServer.call(Miruvor.Raft, {:post, {conn, "test", "nice!"}})

    # send_resp(conn, 200, inspect(resp))
  end

  def put(conn, _params) do
    send_resp(conn, 200, "PUT")
  end

  def delete(conn, _params) do
    send_resp(conn, 200, "DELETE")
  end
end
