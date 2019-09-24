defmodule Mongo.Ecto.MigrationsTest do
  use ExUnit.Case

  # alias Ecto.Integration.TestRepo
  # import Ecto.Query, only: [from: 2]

  # defmodule CreateMigration do
  #   use Ecto.Migration

  #   @table table(:create_table_migration, options: [autoIndexId: false])
  #   @index index(:create_table_migration, [:value], unique: true)

  #   def up do
  #     create(@table)
  #     create(@index)

  #     execute(ping: 1)
  #   end

  #   def down do
  #     drop(@index)
  #     drop(@table)
  #   end
  # end

  # defmodule RenameMigration do
  #   use Ecto.Migration

  #   @table_current table(:posts_migration)
  #   @table_new table(:new_posts_migration)

  #   def up do
  #     create(@table_current)
  #     rename(@table_current, to: @table_new)
  #   end

  #   def down do
  #     drop(@table_new)
  #   end
  # end

  # defmodule NoErrorTableMigration do
  #   use Ecto.Migration

  #   def up do
  #     create_if_not_exists(table(:existing))
  #     create_if_not_exists(table(:existing))
  #   end

  #   def down do
  #     :ok
  #   end
  # end

  # defmodule RenameSchema do
  #   use Ecto.Integration.Schema

  #   schema "rename_migration" do
  #     field :to_be_renamed, :integer
  #     field :was_renamed, :integer
  #   end
  # end

  # defmodule RenameColumnMigration do
  #   use Ecto.Migration

  #   def up do
  #     rename(table(:rename_migration), :to_be_renamed, to: :was_renamed)
  #   end

  #   def down do
  #     drop(table(:rename_migration))
  #   end
  # end

  # defmodule SQLMigration do
  #   use Ecto.Migration

  #   def up do
  #     assert_raise ArgumentError, ~r"does not support SQL statements", fn ->
  #       execute("UPDATE posts SET published_at = NULL")
  #       flush()
  #     end

  #     assert_raise ArgumentError, ~r"does not support SQL statements", fn ->
  #       create(table(:create_table_migration, options: "WITH ?"))
  #       flush()
  #     end
  #   end

  #   def down do
  #     :ok
  #   end
  # end

  # defmodule ReferencesMigration do
  #   use Ecto.Migration

  #   def change do
  #     create table(:reference_migration) do
  #       add(:group_id, references(:groups))
  #     end
  #   end
  # end

  # import Ecto.Migrator, only: [up: 4, down: 4]

  # test "create and drop indexes" do
  #   assert :ok == up(TestRepo, 20_050_906_120_000, CreateMigration, log: false)
  #   assert :ok == down(TestRepo, 20_050_906_120_000, CreateMigration, log: false)
  # end

  # test "raises on SQL migrations" do
  #   assert :ok == up(TestRepo, 20_150_704_120_000, SQLMigration, log: false)
  #   assert :ok == down(TestRepo, 20_150_704_120_000, SQLMigration, log: false)
  # end

  # # TODO add back, once we get the ability to change database from the driver
  # # test "rename table" do
  # #   assert :ok == up(TestRepo, 20150712120000, RenameMigration, log: false)
  # #   assert :ok == down(TestRepo, 20150712120000, RenameMigration, log: false)
  # # end

  # test "create table if not exists does not raise on failure" do
  #   assert :ok == up(TestRepo, 19_850_423_000_001, NoErrorTableMigration, log: false)
  #   assert :ok == down(TestRepo, 19_850_423_000_001, NoErrorTableMigration, log: false)
  # end

  # test "rename column" do
  #   TestRepo.insert!(%RenameSchema{to_be_renamed: 1})
  #   assert :ok == up(TestRepo, 20_150_718_120_000, RenameColumnMigration, log: false)

  #   assert {nil, 1} ==
  #            TestRepo.one(from p in RenameSchema, select: {p.to_be_renamed, p.was_renamed})

  #   :ok = down(TestRepo, 20_150_718_120_000, RenameColumnMigration, log: false)
  # end

  # test "references raise" do
  #   warning =
  #     ExUnit.CaptureIO.capture_io(fn ->
  #       assert :ok == up(TestRepo, 20_150_816_120_000, ReferencesMigration, log: false)
  #     end)

  #   assert warning =~ "does not support references"
  #   assert :ok == down(TestRepo, 20_150_816_120_000, ReferencesMigration, log: false)
  # end
end
