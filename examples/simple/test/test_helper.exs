ExUnit.start()
Logger.configure(level: :info)

defmodule Simple.Case do
  use ExUnit.CaseTemplate

  setup do
    Mongo.Ecto.truncate(Simple.Repo)
    :ok
  end
end
