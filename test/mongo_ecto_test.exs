defmodule Mongo.EctoTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias Ecto.Integration.Post

  import Ecto.Query, only: [from: 2]

  test "command/3" do
    assert Mongo.Ecto.command(TestRepo, ping: 1) == {:ok, [%{"ok" => 1.0}]}
  end

  test "truncate/1" do
    TestRepo.insert!(%Post{})

    Mongo.Ecto.truncate(TestRepo)
    assert [] == TestRepo.all(Post)
  end

  test "javascript" do
    import Mongo.Ecto.Helpers

    TestRepo.insert!(%Post{visits: 1})

    js = javascript("this.visits == count", count: 1)

    assert [%Post{}] = TestRepo.all(from p in Post, where: ^js)
  end
end
