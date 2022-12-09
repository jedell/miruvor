defmodule Miruvor do
  @moduledoc """
  Miruvor keeps the contexts that define your domain
  and business logic.

  Contexts are also responsible for managing your data, regardless
  if it comes from the database, an external API or others.
  """

  @doc """
  Returns the list of child specs to start when the application starts.
  """
  def child_spec(opts) do
    Supervisor.child_spec(opts, id: __MODULE__)
  end

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end


end
