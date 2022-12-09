defmodule MiruvorWeb.Router do
  use MiruvorWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", MiruvorWeb do
    pipe_through :api
    get "/", Controller, :index
    get "/get", Controller, :get
    post "/post", Controller, :post
    put "/put", Controller, :put
    delete "/delete", Controller, :delete


  end
end
