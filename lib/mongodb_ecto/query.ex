defmodule MongodbEcto.Query do
  alias Ecto.Query.QueryExpr

  def all(query, params) do
    check(query.joins, [], query, "MongoDB adapter does not support join clauses")
    check(query.distinct, nil, query, "MongoDB adapter does not support distinct clauses")
    check(query.lock, nil, query, "MongoDB adapter does not support locking")

    # TODO we could support some of those with aggregation pipelines
    check(query.group_bys, [], query, "MongoDB adapter does not support group_by clauses")
    check(query.havings, [], query, "MongoDB adapter does not support having clauses")

    # TODO we could support those with the '$query' syntax
    check(query.order_bys, [], query, "MongoDB adapter does not support order by clauses")

    skip = offset(query.offset)
    batch_size = limit(query.limit)
    projection = select(query.select.fields)
    selector = wheres(query.wheres, List.to_tuple(params))

    {table, _} = query.from
    {table, selector, projection, skip, batch_size}
  end

  defp check(expr, expr, _, _), do: nil
  defp check(_, _, query, message) do
    raise Ecto.QueryError, query: query, message: message
  end

  defp offset(nil), do: 0
  defp offset(int) when is_integer(int), do: int

  defp limit(nil), do: 0
  defp limit(int) when is_integer(int), do: int

  defp select([{:&, [], [0]}]) do
    %{}
  end

  defp wheres(query, params) do
    query
    |> Enum.map(fn %QueryExpr{expr: expr} -> expr(expr, params) end)
    |> Enum.into(%{})
  end

  binary_ops =
    [==: :"$eq", >: :"$gt", >=: :"$gte", <: :"$lt", <=: :"$lte", !=: :"$ne", in: :"$in"]

  @binary_ops Keyword.keys(binary_ops)

  Enum.map(binary_ops, fn {op, mongo_op} ->
    defp translate(unquote(op)), do: unquote(mongo_op)
  end)

  defp expr(int, _) when is_integer(int), do: int
  defp expr(float, _) when is_float(float), do: float
  defp expr(string, _) when is_binary(string), do: string
  defp expr({:^, _, [idx]}, params), do: elem(params, idx)
  defp expr({:in, _, [left, {:^, _, [ix, len]}]}, params) do
    args = Enum.map(ix..ix+len-1, &elem(params, &1))
    {expr(left, params), %{"$in": args}}
  end
  defp expr({op, _, [left, right]}, params) when op in @binary_ops do
    {expr(left, params), Map.put(%{}, translate(op), expr(right, params))}
  end
  defp expr({{:., _, [{:&, _, [0]}, field]}, _, []}, _) when is_atom(field), do: field
  defp expr(list, params) when is_list(list), do: Enum.map(list, &expr(&1, params))
  defp expr({:not, _, [{:in, _, [left, right]}]}, params) do
    {expr(left, params), %{"$nin": expr(right, params)}}
  end
end
