import Config

config :logger,
  level: :debug,
  handle_otp_reports: true,
  handle_sasl_reports: true


config :logger, :console,
  format: "$time [$metadata][$level] $message\n",
  metadata: [:domain, :pid, :registered_name, :partition_id, :offset]
