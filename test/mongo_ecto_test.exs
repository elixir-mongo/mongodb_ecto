defmodule Mongo.EctoTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias Ecto.Integration.Post
  alias Ecto.Integration.Tag

  import Ecto.Query, only: [from: 2]
  import Mongo.Ecto.Helpers

  test "command/3" do
    assert Mongo.Ecto.command(TestRepo, ping: 1) == {:ok, [%{"ok" => 1.0}]}
  end

  test "truncate/1" do
    TestRepo.insert!(%Post{})

    Mongo.Ecto.truncate(TestRepo)
    assert [] == TestRepo.all(Post)
  end

  test "javascript in query" do
    TestRepo.insert!(%Post{visits: 1})

    js = javascript("this.visits == count", count: 1)

    assert [%Post{}] = TestRepo.all(from p in Post, where: ^js)
  end

  test "regex in query" do
    p1 = TestRepo.insert!(%Post{title: "some text"})
    p2 = TestRepo.insert!(%Post{title: "other text"})

    assert [p1] == TestRepo.all(from p in Post, where: fragment(title: ["$regex": "some"]))
    assert [p2] == TestRepo.all(from p in Post, where: fragment(title: ^regex("other")))
  end

  test "retrieve whole document" do
    TestRepo.insert!(%Tag{ints: [1, 2, 3]})

    query = from t in Tag, where: 1 in t.ints, select: fragment("ints.$": 1)
    assert [%{"ints" => [1]}] = TestRepo.all(query)
  end
end
