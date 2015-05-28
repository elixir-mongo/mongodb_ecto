defmodule MongodbEcto.Query do

  def all(query) do
    {{table, _}} = query.sources
    {table, {}, {}, 0}
  end
end
