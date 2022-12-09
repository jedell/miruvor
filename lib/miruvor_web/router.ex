defmodule MiruvorWeb.Router do
  use MiruvorWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", MiruvorWeb do
    pipe_through :api
    get "/", Controller, :index
    get "/read/:key", Controller, :read
    post "/write/:key/:value", Controller, :write
    put "/update/:key/:value", Controller, :update
    delete "/delete/:key", Controller, :delete


  end
end
