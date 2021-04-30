# System.at_exit fn _ -> Logger.flush end
Logger.configure(level: :info)

ExUnit.start(
  exclude: [
    :composite_pk,
    :cross_join,
    :decimal_type,
    :delete_with_join,
    :foreign_key_constraint,
    :id_type,
    :invalid_prefix,
    :join,
    :left_join,
    :read_after_writes,
    :returning,
    :right_join,
    :sql_fragments,
    :transaction,
    :update_with_join,
    :uses_usec,

    # Unsure?
    :group_by,
    :sub_query,
    :preload,

    # TODO: Turn these back on
    :with_conflict_target,
    :without_conflict_target
  ]
)

Application.put_env(:ecto, :primary_key_type, :binary_id)
Application.put_env(:ecto, :async_integration_tests, false)

# Code.require_file("../deps/ecto_sql/integration_test/support/repo.exs", __DIR__)

# Basic test repo
alias Ecto.Integration.TestRepo

Application.put_env(
  :ecto,
  TestRepo,
  adapter: Mongo.Ecto,
  url: "ecto://localhost:27017/ecto_test",
  pool_size: 1
)

Application.put_env(
  :ecto,
  Ecto.Integration.PoolRepo,
  adapter: Mongo.Ecto,
  url: "ecto://localhost:27017/ecto_test",
  pool_size: 5
)

defmodule Ecto.Integration.Repo do
  defmacro __using__(opts) do
    quote do
      use Ecto.Repo, unquote(opts)

      @query_event __MODULE__
                   |> Module.split()
                   |> Enum.map(&(&1 |> Macro.underscore() |> String.to_atom()))
                   |> Kernel.++([:query])

      def init(_, opts) do
        fun = &Ecto.Integration.Repo.handle_event/4
        :telemetry.attach_many(__MODULE__, [[:custom], @query_event], fun, :ok)
        {:ok, opts}
      end
    end
  end

  def handle_event(event, latency, metadata, _config) do
    handler = Process.delete(:telemetry) || fn _, _, _ -> :ok end
    handler.(event, latency, metadata)
  end
end

defmodule Ecto.Integration.TestRepo do
  use Ecto.Integration.Repo, otp_app: :ecto, adapter: Mongo.Ecto

  def uuid do
    Ecto.UUID
  end
end

defmodule Ecto.Integration.PoolRepo do
  use Ecto.Integration.Repo, otp_app: :ecto, adapter: Mongo.Ecto
end

defmodule Ecto.Integration.Case do
  use ExUnit.CaseTemplate

  alias Ecto.Integration.TestRepo
  alias Ecto.Integration.PoolRepo

  setup_all do
    :ok
  end

  setup do
    Mongo.Ecto.truncate(TestRepo)
    :ok
  end
end

Code.require_file("../deps/ecto/integration_test/support/types.exs", __DIR__)
Code.require_file("../deps/ecto/integration_test/support/schemas.exs", __DIR__)

_ = Mongo.Ecto.storage_down(TestRepo.config())

:ok = Mongo.Ecto.storage_up(TestRepo.config())

{:ok, _pid} = TestRepo.start_link()
:ok = TestRepo.stop(:infinity)
{:ok, _pid} = TestRepo.start_link()
{:ok, _pid} = Ecto.Integration.PoolRepo.start_link()

# We capture_io, because of warnings on references
# ExUnit.CaptureIO.capture_io(fn ->
#  :ok = Ecto.Migrator.up(TestRepo, 0, Ecto.Integration.Migration, log: false)
# end)
