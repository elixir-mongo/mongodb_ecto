defmodule MongodbEcto.Query do
  alias Ecto.Query.QueryExpr
  alias Ecto.Query.Tagged

  def all(query, params) do
    check(query.joins, [], query, "MongoDB adapter does not support join clauses")
    check(query.distinct, nil, query, "MongoDB adapter does not support distinct clauses")
    check(query.lock, nil, query, "MongoDB adapter does not support locking")

    # TODO we could support some of those with aggregation pipelines
    check(query.group_bys, [], query, "MongoDB adapter does not support group_by clauses")
    check(query.havings, [], query, "MongoDB adapter does not support having clauses")

    params = List.to_tuple(params)
    {table, model} = query.from

    skip = offset_limit(query.offset)
    batch_size = offset_limit(query.limit)
    projection = select(query.select.fields, model, [])
    selector = wheres(query.wheres, params)

    if query.order_bys != [] do
      orderby = order_bys(query.order_bys, params)
      selector = %{"$query": selector, "$orderby": orderby}
    end

    {table, selector, projection, skip, batch_size}
  end

  defp put_if_not_empty(map, _key, value) when value == %{}, do: map
  defp put_if_not_empty(map, key, value), do: Map.put(map, key, value)

  defp check(expr, expr, _, _), do: nil
  defp check(_, _, query, message) do
    raise Ecto.QueryError, query: query, message: message
  end

  defp offset_limit(nil), do: 0
  defp offset_limit(%QueryExpr{expr: int}) when is_integer(int), do: int

  defp select([{:&, _, [0]}], _model, []) do
    %{}
  end
  defp select([], _model, fields) do
    Enum.into(fields, %{}, fn field -> {field, 1} end)
  end
  defp select([{:&, _, [0]} | rest], model, fields) do
    select(rest, model, model.__schema__(:fields) ++ fields)
  end
  defp select([{{:., _, [{:&, _, [0]}, field]}, _, []} | rest], model, fields) do
    select(rest, model, [field | fields])
  end
  # FIXME can we do something better than simply pass on the values here, and
  #       then insert them when processing the query result?
  defp select([_ | rest], model, fields) do
    select(rest, model, fields)
  end

  # TODO with two clauses on the same field we should join them with an '$and'
  defp wheres(query, params) do
    query
    |> Enum.map(fn %QueryExpr{expr: expr} -> expr(expr, params) end)
    |> Enum.into(%{})
  end

  defp order_bys(order_bys, params) do
    order_bys
    |> Enum.flat_map(fn
      %QueryExpr{expr: expr} ->
        Enum.map(expr, &order_by_expr(&1, params))
    end)
    |> Enum.into(%{})
  end

  defp order_by_expr({:asc,  expr}, params), do: {expr(expr, params),  1}
  defp order_by_expr({:desc, expr}, params), do: {expr(expr, params), -1}

  binary_ops =
    [>: :"$gt", >=: :"$gte", <: :"$lt", <=: :"$lte", !=: :"$ne", in: :"$in"]
  bool_ops =
    [and: :"$and", or: :"$or"]

  @binary_ops Keyword.keys(binary_ops)
  @bool_ops Keyword.keys(bool_ops)

  Enum.map(binary_ops ++ bool_ops, fn {op, mongo_op} ->
    defp translate(unquote(op)), do: unquote(mongo_op)
  end)

  defp expr(int, _) when is_integer(int), do: int
  defp expr(float, _) when is_float(float), do: float
  defp expr(string, _) when is_binary(string), do: string
  defp expr({:^, _, [idx]}, params), do: elem(params, idx)
  defp expr({:in, _, [left, {:^, _, [ix, len]}]}, params) do
    args = Enum.map(ix..ix+len-1, &elem(params, &1)) |> Enum.map(&expr(&1, params))
    {expr(left, params), %{"$in": args}}
  end
  defp expr({:is_nil, _, [expr]}, params) do
    {expr(expr, params), nil}
  end
  defp expr({:==, _, [left, right]}, params) do
    {expr(left, params), expr(right, params)}
  end
  defp expr({op, _, [left, right]}, params) when op in @binary_ops do
    {expr(left, params), Map.put(%{}, translate(op), expr(right, params))}
  end
  defp expr({{:., _, [{:&, _, [0]}, field]}, _, []}, _) when is_atom(field), do: field
  defp expr(list, params) when is_list(list), do: Enum.map(list, &expr(&1, params))
  defp expr({:not, _, [{:in, _, [left, right]}]}, params) do
    {expr(left, params), %{"$nin": expr(right, params)}}
  end
  defp expr({:not, _, [{:is_nil, _, [expr]}]}, params) do
    {expr(expr, params), %{"$neq": nil}}
  end
  defp expr({:not, _, [expr]}, params) do
    {key, value} = expr(expr, params)
    {:"$not", Map.put(%{}, key, value)}
  end
  defp expr({op, _, args}, params) when op in @bool_ops do
    {translate(op), Enum.map(args, &expr(&1, params))}
  end
  defp expr(%Tagged{} = value, _), do: value
  defp expr(atom, _) when is_atom(atom), do: atom
end
