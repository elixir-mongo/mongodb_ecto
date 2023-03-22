# 10/10 pass

defmodule Mongo.EctoTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.{
    Post,
    Tag,
    TestRepo
  }

  import Ecto.Query, only: [from: 2]
  import Mongo.Ecto.Helpers

  test "command/3" do
    assert %{"ok" => 1.0} = Mongo.Ecto.command(TestRepo, ping: 1)
  end

  test "truncate/2" do
    TestRepo.insert!(%Post{})

    case System.get_env("MONGOVERSION") do
      version when version in ["2.6.12", "3.0.15"] ->
        nil

      _ ->
        # test with Views
        Mongo.Ecto.command(TestRepo, %{create: "view", viewOn: "posts", pipeline: []})
    end

    Mongo.Ecto.truncate(TestRepo)

    assert [] == TestRepo.all(Post)
  end

  test "regex in query" do
    p1 = TestRepo.insert!(%Post{title: "some text"})
    p2 = TestRepo.insert!(%Post{title: "other text"})

    assert [p1] == TestRepo.all(from p in Post, where: fragment(title: ["$regex": "some"]))

    assert [p2] ==
             TestRepo.all(
               from p in Post,
                 where: fragment(title: ^regex("other")) and fragment(title: ^regex("text"))
             )
  end

  test "retrieve whole document" do
    TestRepo.insert!(%Tag{ints: [1, 2, 3]})

    query = from t in Tag, where: 1 in t.ints, select: fragment("ints.$": 1)
    assert [%{"ints" => [1]}] = TestRepo.all(query)
  end

  test "count" do
    TestRepo.insert!(%Post{visits: 1})

    query = from p in Post, where: p.visits == 1, select: count(p.id)
    assert 1 == TestRepo.one(query)
  end

  test "min" do
    TestRepo.insert!(%Post{visits: 5})
    TestRepo.insert!(%Post{visits: 10})
    TestRepo.insert!(%Post{visits: 15})

    query = from p in Post, where: p.visits >= 10, select: min(p.visits)
    assert 10 == TestRepo.one(query)
  end

  test "max" do
    TestRepo.insert!(%Post{visits: 15})
    TestRepo.insert!(%Post{visits: 10})
    TestRepo.insert!(%Post{visits: 5})

    query = from p in Post, offset: 1, select: max(p.visits)
    assert 10 == TestRepo.one(query)
  end

  test "sum" do
    TestRepo.insert!(%Post{visits: 15})
    TestRepo.insert!(%Post{visits: 10})
    TestRepo.insert!(%Post{visits: 5})

    query = from p in Post, limit: 2, select: sum(p.visits)
    assert 25 == TestRepo.one(query)
  end

  test "avg" do
    TestRepo.insert!(%Post{visits: 15})
    TestRepo.insert!(%Post{visits: 10})
    TestRepo.insert!(%Post{visits: 5})

    query = from p in Post, select: avg(p.visits)
    assert 10 == TestRepo.one(query)
  end

  # test "partial update in map" do
  #   post = TestRepo.insert!(%Post{meta: %{author: %{name: "michal"}, other: "value"}})
  #   TestRepo.update_all(Post, set: [meta: change_map("author.name", "michal")])

  #   assert TestRepo.get!(Post, post.id).meta ==
  #     %{"author" => %{"name" => "michal"}, "other" => "value"}

  #   order = Ecto.Changeset.change(%Order{}, item: %Item{price: 1})
  #   order = TestRepo.insert!(order)
  #   TestRepo.update_all(Order, set: [item: change_map("price", 10)])

  #   assert TestRepo.get!(Order, order.id).item.price == 10
  # end

  # test "partial update in array" do
  #   tag = Ecto.Changeset.change(%Tag{}, items: [%Item{price: 1}])
  #   tag = TestRepo.insert!(tag)
  #   TestRepo.update_all(Tag, set: [items: change_array(0, "price", 10)])

  #   [item] = TestRepo.get!(Tag, tag.id).items
  #   assert item.price == 10
  # end

  test "decode empty map to map" do
    post = TestRepo.insert!(%Post{meta: %{}})
    assert post.meta == %{}

    post = TestRepo.get(Post, post.id)
    assert post.meta == %{}
  end

  # test "list_collections does not include schema collection" do
  #  schema_collection = Ecto.Migration.SchemaMigration.__schema__(:source)

  #  refute schema_collection in Mongo.Ecto.list_collections(TestRepo)
  # end
end
