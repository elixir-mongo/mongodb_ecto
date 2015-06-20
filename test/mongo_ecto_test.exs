defmodule Mongo.EctoTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias Ecto.Integration.Post

  import Ecto.Query, only: [from: 2]

  test "command/3" do
    assert Mongo.Ecto.command(TestRepo, ping: 1) == {:ok, [%{"ok" => 1.0}]}
  end

  test "truncate/1" do
    TestRepo.insert(%Post{})

    Mongo.Ecto.truncate(TestRepo)
    assert [] == TestRepo.all(Post)
  end

  test "javascript" do
    TestRepo.insert(%Post{visits: 1})

    js = %BSON.JavaScript{code: "this.visits === 1"}

    assert [%Post{}] = TestRepo.all(from p in Post, where: type(^js, Mongo.Ecto.JavaScript))
  end
end
