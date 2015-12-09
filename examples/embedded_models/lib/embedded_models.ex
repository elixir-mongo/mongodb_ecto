defmodule EmbeddedModels.App do
  use Application

  def start(_type, _args) do
    import Supervisor.Spec

    tree = [worker(EmbeddedModels.Repo, [])]

    opts = [name: EmbeddedModels.Sup, strategy: :one_for_one]
    Supervisor.start_link(tree, opts)
  end
end

defmodule EmbeddedModels.Repo do
  use Ecto.Repo, otp_app: :embedded_models
end

defmodule EmbeddedModels.Permalink do
  use Ecto.Model

  embedded_schema do
    field :url
    timestamps
  end
end

defmodule EmbeddedModels.Post do
  use Ecto.Model

  @primary_key {:id, :binary_id, autogenerate: true}

  schema "posts" do
    field :title
    field :body
    has_many :comments, EmbeddedModels.Comment
    embeds_many :permalinks, EmbeddedModels.Permalink
    timestamps
  end
end

defmodule EmbeddedModels do
  alias EmbeddedModels.Repo
  alias EmbeddedModels.Permalink
  alias EmbeddedModels.Post

  def insert do
    # Generate a changeset for the post
    changeset = Ecto.Changeset.change(%Post{})

    # Let's track the new permalinks
    changeset = Ecto.Changeset.put_change(changeset, :permalinks,
                                          [%Permalink{url: "example.com/thebest"},
                                           %Permalink{url: "another.com/mostaccessed"}]
    )

    # Now let's insert the post with permalinks at once!
    Repo.insert!(changeset)
  end
end
