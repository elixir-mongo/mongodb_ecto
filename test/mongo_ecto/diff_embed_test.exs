defmodule Mongo.Ecto.DiffEmbedTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo

  import Ecto.Query, only: [from: 2]

  alias __MODULE__.Order
  alias __MODULE__.Item
  alias __MODULE__.SubItem

  defmodule Order do
    use Ecto.Integration.Model

    schema "orders" do
      embeds_one :item, Item, strategy: :diff
    end
  end

  defmodule Item do
    use Ecto.Integration.Model

    @primary_key {:id, :binary_id, autogenerate: true}
    schema "whatever" do
      field :price, :integer
      embeds_one :sub_item, SubItem, strategy: :diff
    end
  end

  defmodule SubItem do
    use Ecto.Integration.Model

    @primary_key {:id, :binary_id, autogenerate: true}
    schema "whatever" do
      field :name, :string
    end
  end

  test "embeds_one insert" do
    model = TestRepo.insert!(%Order{})
    refute model.item

    model = Ecto.Changeset.change(%Order{}, item: %Item{price: 1})
    model = TestRepo.insert!(model)
    assert model.item.price == 1
    assert model.item.id

    item = Ecto.Changeset.change(%Item{}, sub_item: %SubItem{name: "abc"})
    model = Ecto.Changeset.change(%Order{}, item: item)
    model = TestRepo.insert!(model)
    assert model.item.sub_item.name == "abc"
    assert model.item.sub_item.id
  end

  test "embeds_one update" do
    model = TestRepo.insert!(%Order{})

    changeset = Ecto.Changeset.change(model, item: %Item{price: 1})
    model = TestRepo.update!(changeset)
    model = TestRepo.get!(Order, model.id)
    assert model.item.price == 1
    assert model.item.id

    item_change = Ecto.Changeset.change(model.item, sub_item: %SubItem{name: "abc"})
    changeset = Ecto.Changeset.change(model, item: item_change)
    model = TestRepo.update!(changeset)
    model = TestRepo.get!(Order, model.id)
    assert model.item.sub_item.name == "abc"
    assert model.item.sub_item.id
    assert model.item.price == 1

    sub_item_change = Ecto.Changeset.change(model.item.sub_item, name: "xyz")
    item_change = Ecto.Changeset.change(model.item, sub_item: sub_item_change)
    changeset = Ecto.Changeset.change(model, item: item_change)
    model = TestRepo.update!(changeset)
    model = TestRepo.get!(Order, model.id)
    assert model.item.sub_item.name == "xyz"

    changeset = Ecto.Changeset.change(model, item: nil)
    model = TestRepo.update!(changeset)
    model = TestRepo.get!(Order, model.id)
    refute model.item
  end

  test "embeds_one delete" do
    changeset = Ecto.Changeset.change(%Order{}, item: %Item{price: 1})
    model = TestRepo.insert!(changeset)

    changeset = Ecto.Changeset.change(model, item: nil)
    model = TestRepo.update!(changeset)
    model = TestRepo.get!(Order, model.id)
    refute model.item
  end
end
