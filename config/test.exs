import Config

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :miruvor, MiruvorWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "6ZzxNw0dJYWjR/RDOtvxUySsxMEkzqxZZcqEyGRT7uBDUb2hmSDfDr9yTywm3+19",
  server: false

# Print only warnings and errors during test
config :logger, level: :debug

# Initialize plugs at runtime for faster test compilation
config :phoenix, :plug_init_mode, :runtime
