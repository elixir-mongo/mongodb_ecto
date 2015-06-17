defmodule Mongo.EctoTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias Ecto.Integration.Post

  test "command/3" do
    assert Mongo.Ecto.command(TestRepo, ping: 1) == {:ok, [%{"ok" => 1.0}]}
  end

  test "truncate/1" do
    TestRepo.insert(%Post{})

    Mongo.Ecto.truncate(TestRepo)
    assert [] == TestRepo.all(Post)
  end
end
