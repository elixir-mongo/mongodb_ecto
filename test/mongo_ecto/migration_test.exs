defmodule Mongo.Ecto.MigrationTest do
  use ExUnit.Case, async: false

  alias Mongo.Ecto.Migration

  # We test execute_ddl clause matching directly — the MongoDB calls
  # are tested via integration in the installer tests.
  # Here we verify the function heads compile and return {:ok, []}.

  test "supports_ddl_transaction? returns false" do
    assert Migration.supports_ddl_transaction?() == false
  end

  test "execute_ddl no-ops for alter table" do
    assert {:ok, []} = Migration.execute_ddl(:fake_meta, {:alter, %Ecto.Migration.Table{name: :users}, []}, [])
  end

  test "execute_ddl no-ops for rename table" do
    assert {:ok, []} = Migration.execute_ddl(:fake_meta, {:rename, %Ecto.Migration.Table{name: :users}, %Ecto.Migration.Table{name: :accounts}}, [])
  end

  test "execute_ddl no-ops for drop_if_exists table" do
    assert {:ok, []} = Migration.execute_ddl(:fake_meta, {:drop_if_exists, %Ecto.Migration.Table{name: :users}, :restrict}, [])
  end

  test "execute_ddl no-ops for string commands" do
    assert {:ok, []} = Migration.execute_ddl(:fake_meta, "SELECT 1", [])
  end

  test "lock_for_migrations calls fun and returns its value" do
    assert Migration.lock_for_migrations(:meta, [], fn -> :my_result end) == :my_result
  end

  test "execute_ddl no-ops for rename column" do
    assert {:ok, []} = Migration.execute_ddl(:fake_meta, {:rename, %Ecto.Migration.Table{name: :users}, :old_col, :new_col}, [])
  end

  test "execute_ddl no-ops for rename index" do
    assert {:ok, []} = Migration.execute_ddl(:fake_meta, {:rename, %Ecto.Migration.Index{table: :users, columns: [:email], name: :idx}, :new_idx_name}, [])
  end

  test "execute_ddl no-ops for create_if_not_exists index" do
    assert {:ok, []} = Migration.execute_ddl(:fake_meta, {:create_if_not_exists, %Ecto.Migration.Index{table: :users, columns: [:email], name: nil, unique: false, concurrently: false, using: nil, prefix: nil, include: [], nulls_distinct: nil, options: nil, comment: nil, where: nil}}, [])
  end

  describe "create and drop collection" do
    setup do
      {:ok, pid} = Mongo.start_link(url: "mongodb://localhost:27017", database: "mongo_ecto_migration_test")
      Process.unlink(pid)
      meta = %{pid: pid, telemetry: {__MODULE__, :debug, [:mongo_ecto, :migration, :query]}, opts: []}
      on_exit(fn ->
        Mongo.command!(pid, dropDatabase: 1)
        GenServer.stop(pid)
      end)
      {:ok, meta: meta}
    end

    test "execute_ddl :create table creates a MongoDB collection", %{meta: meta} do
      assert {:ok, []} =
               Mongo.Ecto.Migration.execute_ddl(
                 meta,
                 {:create, %Ecto.Migration.Table{name: :migration_test_users}, []},
                 []
               )

      collections = Mongo.show_collections(meta.pid) |> Enum.to_list()
      assert "migration_test_users" in collections
    end

    test "execute_ddl :drop table drops a MongoDB collection", %{meta: meta} do
      Mongo.command!(meta.pid, create: "migration_test_drop_me")

      assert {:ok, []} =
               Mongo.Ecto.Migration.execute_ddl(
                 meta,
                 {:drop, %Ecto.Migration.Table{name: :migration_test_drop_me}, :restrict},
                 []
               )

      collections = Mongo.show_collections(meta.pid) |> Enum.to_list()
      refute "migration_test_drop_me" in collections
    end

    test "execute_ddl :create is idempotent (collection already exists)", %{meta: meta} do
      Mongo.create(meta.pid, "already_exists_collection")

      assert {:ok, []} =
               Mongo.Ecto.Migration.execute_ddl(
                 meta,
                 {:create, %Ecto.Migration.Table{name: :already_exists_collection}, []},
                 []
               )
    end
  end

  describe "schema_migrations" do
    setup do
      {:ok, pid} = Mongo.start_link(url: "mongodb://localhost:27017", database: "mongo_ecto_schema_migrations_test")
      Process.unlink(pid)
      meta = %{pid: pid, telemetry: {__MODULE__, :debug, [:mongo_ecto, :migration, :query]}, opts: []}
      on_exit(fn ->
        Mongo.command!(pid, dropDatabase: 1)
        GenServer.stop(pid)
      end)
      {:ok, meta: meta}
    end

    test "execute_ddl create_if_not_exists creates collection and unique index on pk field", %{meta: meta} do
      commands = [{:add, :version, :bigint, [primary_key: true]}]

      assert {:ok, []} =
               Migration.execute_ddl(
                 meta,
                 {:create_if_not_exists, %Ecto.Migration.Table{name: :schema_migrations}, commands},
                 []
               )

      collections = Mongo.show_collections(meta.pid) |> Enum.to_list()
      assert "schema_migrations" in collections

      indexes = Mongo.list_indexes(meta.pid, "schema_migrations") |> Enum.to_list()
      index_names = Enum.map(indexes, & &1["name"])
      assert "schema_migrations_pk" in index_names

      pk_index = Enum.find(indexes, fn idx -> idx["name"] == "schema_migrations_pk" end)
      assert pk_index["unique"] == true
      assert pk_index["key"] == %{"version" => 1}
    end

    test "execute_ddl create_if_not_exists is idempotent", %{meta: meta} do
      commands = [{:add, :version, :bigint, [primary_key: true]}]

      assert {:ok, []} =
               Migration.execute_ddl(
                 meta,
                 {:create_if_not_exists, %Ecto.Migration.Table{name: :schema_migrations}, commands},
                 []
               )

      # Second call should also succeed (collection already exists)
      assert {:ok, []} =
               Migration.execute_ddl(
                 meta,
                 {:create_if_not_exists, %Ecto.Migration.Table{name: :schema_migrations}, commands},
                 []
               )
    end

    test "execute_ddl create_if_not_exists with no pk fields creates collection only", %{meta: meta} do
      assert {:ok, []} =
               Migration.execute_ddl(
                 meta,
                 {:create_if_not_exists, %Ecto.Migration.Table{name: :no_pk_collection}, []},
                 []
               )

      collections = Mongo.show_collections(meta.pid) |> Enum.to_list()
      assert "no_pk_collection" in collections
    end
  end

  describe "create and drop index" do
    setup do
      {:ok, pid} = Mongo.start_link(url: "mongodb://localhost:27017", database: "mongo_ecto_index_test")
      Process.unlink(pid)
      # ensure collection exists before indexing
      Mongo.create(pid, "indexed_collection")
      meta = %{pid: pid, telemetry: {__MODULE__, :debug, [:mongo_ecto, :migration, :query]}, opts: []}
      on_exit(fn ->
        Mongo.command!(pid, dropDatabase: 1)
        GenServer.stop(pid)
      end)
      {:ok, meta: meta}
    end

    test "execute_ddl creates a unique index", %{meta: meta} do
      assert {:ok, []} =
               Mongo.Ecto.Migration.execute_ddl(
                 meta,
                 {:create,
                  %Ecto.Migration.Index{
                    table: :indexed_collection,
                    columns: [:email],
                    name: :indexed_collection_email_index,
                    unique: true,
                    concurrently: false,
                    using: nil,
                    prefix: nil,
                    include: [],
                    nulls_distinct: nil,
                    options: nil,
                    comment: nil,
                    where: nil
                  }},
                 []
               )

      indexes = Mongo.list_indexes(meta.pid, "indexed_collection") |> Enum.to_list()
      index_names = Enum.map(indexes, & &1["name"])
      assert "indexed_collection_email_index" in index_names
    end

    test "execute_ddl drops an index", %{meta: meta} do
      Mongo.create_indexes(meta.pid, "indexed_collection", [
        %{key: %{username: 1}, name: "indexed_collection_username_index"}
      ])

      assert {:ok, []} =
               Mongo.Ecto.Migration.execute_ddl(
                 meta,
                 {:drop,
                  %Ecto.Migration.Index{
                    table: :indexed_collection,
                    columns: [:username],
                    name: :indexed_collection_username_index,
                    unique: false,
                    concurrently: false,
                    using: nil,
                    prefix: nil,
                    include: [],
                    nulls_distinct: nil,
                    options: nil,
                    comment: nil,
                    where: nil
                  }, :restrict},
                 []
               )

      indexes = Mongo.list_indexes(meta.pid, "indexed_collection") |> Enum.to_list()
      index_names = Enum.map(indexes, & &1["name"])
      refute "indexed_collection_username_index" in index_names
    end

    test "execute_ddl generates index name when not specified", %{meta: meta} do
      assert {:ok, []} =
               Mongo.Ecto.Migration.execute_ddl(
                 meta,
                 {:create,
                  %Ecto.Migration.Index{
                    table: :indexed_collection,
                    columns: [:first_name, :last_name],
                    name: nil,
                    unique: false,
                    concurrently: false,
                    using: nil,
                    prefix: nil,
                    include: [],
                    nulls_distinct: nil,
                    options: nil,
                    comment: nil,
                    where: nil
                  }},
                 []
               )

      indexes = Mongo.list_indexes(meta.pid, "indexed_collection") |> Enum.to_list()
      index_names = Enum.map(indexes, & &1["name"])
      assert "indexed_collection_first_name_last_name_index" in index_names
    end
  end
end
