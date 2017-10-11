# Alternative, in ecto_test.exs
# Code.require_file "../../deps/ecto/integration_test/sql/stream.exs", __DIR__
# Code.require_file "../deps/ecto/integration_test/sql/stream.exs", __DIR__
#  However this assumes everything can be run in a transaction, so it fails
#  a precondition of runnability.
# So taking as much of the content as is viable and re-wiring it for mongo

defmodule Mongo.Ecto.Integration.StreamTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias Ecto.Integration.Post
  alias Ecto.Integration.Comment
  import Ecto.Query

  test "stream empty" do
    assert [] == TestRepo.stream(Post)
      |> Enum.to_list()

    assert [] == TestRepo.stream(from p in Post)
      |> Enum.to_list()
  end

  test "stream without schema" do
    %Post{} = TestRepo.insert!(%Post{title: "title1"})
    %Post{} = TestRepo.insert!(%Post{title: "title2"})

    assert ["title1", "title2"] == TestRepo.stream(from(p in "posts", order_by: p.title, select: p.title))
      |> Enum.to_list()
  end

  test "stream with assoc" do
    p1 = TestRepo.insert!(%Post{title: "1"})

    %Comment{id: cid1} = TestRepo.insert!(%Comment{text: "1", post_id: p1.id})
    %Comment{id: cid2} = TestRepo.insert!(%Comment{text: "2", post_id: p1.id})

    stream = TestRepo.stream(Ecto.assoc(p1, :comments))
    assert [c1, c2] = Enum.to_list(stream)
    assert c1.id == cid1
    assert c2.id == cid2
  end

  test "stream with preload" do
    p1 = TestRepo.insert!(%Post{title: "1"})
    p2 = TestRepo.insert!(%Post{title: "2"})
    TestRepo.insert!(%Post{title: "3"})

    %Comment{id: cid1} = TestRepo.insert!(%Comment{text: "1", post_id: p1.id})
    %Comment{id: cid2} = TestRepo.insert!(%Comment{text: "2", post_id: p1.id})
    %Comment{id: cid3} = TestRepo.insert!(%Comment{text: "3", post_id: p2.id})
    %Comment{id: cid4} = TestRepo.insert!(%Comment{text: "4", post_id: p2.id})

    assert [p1, p2, p3] = from(p in Post, preload: [:comments], select: p)
        |> TestRepo.stream([max_rows: 2])
        |> sort_by_id()
    assert [%Comment{id: ^cid1}, %Comment{id: ^cid2}] = p1.comments |> sort_by_id
    assert [%Comment{id: ^cid3}, %Comment{id: ^cid4}] = p2.comments |> sort_by_id
    assert [] == p3.comments
  end

  defp sort_by_id(values) do
    Enum.sort_by(values, &(&1.id))
  end


end
