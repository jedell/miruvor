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

  def read(conn, %{"key" => key} = params) do
    resp = Miruvor.Raft.read(key)

    # should return correct value
    send_resp(conn, 200, inspect(resp))
  end

  def write(conn, %{"key" => key, "value" => value} = params) do
    resp = Miruvor.Raft.write(key, value)

    send_resp(conn, 200, inspect(resp))
  end

  def update(conn, _params) do
    send_resp(conn, 200, "PUT")
  end

  def delete(conn, _params) do
    send_resp(conn, 200, "DELETE")
  end
end
