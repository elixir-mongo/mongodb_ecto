defmodule Mongo.Ecto.MigrationsTest do
  use ExUnit.Case

  alias Ecto.Integration.TestRepo

  defmodule CreateMigration do
    use Ecto.Migration

    @table table(:create_table_migration, options: [capped: true, size: 1024])
    @index index(:create_table_migration, [:value], unique: true)

    def up do
      refute exists? @table
      create @table
      refute exists? @index
      create @index
      assert exists? @table
      assert exists? @index

      execute ping: 1
    end

    def down do
      assert exists? @index
      assert exists? @table
      drop @index
      refute exists? @index
      drop @table
      refute exists? @table
    end
  end

  defmodule SQLMigration do
    use Ecto.Migration

    def up do
      assert_raise ArgumentError, ~r"does not support SQL statements", fn ->
        execute "UPDATE posts SET published_at = NULL"
      end

      assert_raise ArgumentError, ~r"does not support SQL statements", fn ->
        create table(:create_table_migration, options: "WITH ?")
      end
    end

    def down do
      :ok
    end
  end

  import Ecto.Migrator, only: [up: 4, down: 4]

  test "create and drop indexes" do
    assert :ok == up(TestRepo, 20050906120000, CreateMigration, log: false)
  after
    assert :ok == down(TestRepo, 20050906120000, CreateMigration, log: false)
  end

  test "raises on SQL migrations" do
    assert :ok == up(TestRepo, 20150704120000, SQLMigration, log: false)
  end
end
