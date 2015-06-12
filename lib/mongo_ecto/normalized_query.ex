defmodule Mongo.Ecto.NormalizedQuery do

  defstruct [from: {nil, nil, nil}, query_order: %{}, projection: %{},
             fields: [], command: %{}, num_skip: 0, num_return: 0, params: {}]

  import Mongo.Ecto.Encoder
  alias Ecto.Query

  def from_query(%Query{} = original, command, params) do
    normalized = from_query(original, params)
    command    = command(command, normalized.params, normalized.from)

    %__MODULE__{normalized | command: command}
  end

  def from_query(%Query{} = original, params) do
    check_query(original)

    params      = List.to_tuple(params)
    from        = from(original)
    query_order = query_order(original, params, from)
    projection  = projection(original.select, from)
    fields      = fields(original.select, params)
    num_skip    = num_skip(original)
    num_return  = num_return(original)

    %__MODULE__{from: from, query_order: query_order, projection: projection,
                fields: fields, num_skip: num_skip, num_return: num_return,
                params: params}
  end

  defp from(%Query{from: {coll, model}}) do
    {coll, model, primary_key(model)}
  end

  defp query_order(original, params, from) do
    query = query(original, params, from)
    order = order(original, from)
    query_order(query, order)
  end

  defp query_order(query, order) when order == %{}, do: query
  defp query_order(query, order), do: %{"$query": query, "$orderby": order}

  defp projection(nil, _from) do
    %{}
  end
  defp projection(%Query.SelectExpr{fields: [{:&, _, [0]}]}, _from) do
    %{}
  end
  defp projection(%Query.SelectExpr{fields: fields}, {_coll, model, pk}) do
    Enum.flat_map(fields, fn
      {:&, _, [0]} ->
        model.__schema__(:fields)
      {{:., _, [{:&, _, [0]}, field]}, _, []} when field == pk ->
        [:_id]
      {{:., _, [{:&, _, [0]}, field]}, _, []} ->
        [field]
      _value ->
        # We skip all values and then add them when constructing return result
        []
    end)
    |> Enum.into(%{}, &{&1, true})
  end

  defp fields(nil, _params) do
    []
  end
  defp fields(%Query.SelectExpr{fields: fields}, params) do
    Enum.map(fields, fn
      %Query.Tagged{value: {:^, _, [idx]}} ->
        params |> elem(idx) |> encode_value(params)
      value ->
        value
    end)
  end

  defp num_skip(%Query{offset: offset}) do
    offset_limit(offset)
  end

  defp num_return(%Query{limit: limit}) do
    offset_limit(limit)
  end

  defp query(%Query{wheres: wheres}, params, {_coll, _model, pk}) do
    Enum.into(wheres, %{}, fn %Query.QueryExpr{expr: expr} ->
      pair(expr, params, pk)
    end)
  end

  defp order(%Query{order_bys: order_bys}, {_coll, _model, pk}) do
    order_bys
    |> Enum.flat_map(fn %Query.QueryExpr{expr: expr} -> expr end)
    |> Enum.into(%{}, &order_by_expr(&1, pk))
  end

  defp command(command, params, {_coll, _model, pk}) do
    encode_document(command, params, pk)
  end

  defp offset_limit(nil), do: 0
  defp offset_limit(%Query.QueryExpr{expr: int}) when is_integer(int), do: int

  defp primary_key(nil), do: nil
  defp primary_key(model) do
    case model.__schema__(:primary_key) do
      [pk] -> pk
      keys ->
        raise ArgumentError, "MongoDB does not support multiple primary keys " <>
                             "and #{inspect keys} were defined in #{inspect model}."
    end
  end

  defp order_by_expr({:asc,  expr}, pk), do: {field(expr, pk),  1}
  defp order_by_expr({:desc, expr}, pk), do: {field(expr, pk), -1}

  defp check_query(query) do
    check(query.joins, [], query, "MongoDB adapter does not support join clauses")
    check(query.distinct, nil, query, "MongoDB adapter does not support distinct clauses")
    check(query.lock, nil, query, "MongoDB adapter does not support locking")
    check(query.group_bys, [], query, "MongoDB adapter does not support group_by clauses")
    check(query.havings, [], query, "MongoDB adapter does not support having clauses")
  end

  defp check(expr, expr, _, _), do: nil
  defp check(_, _, query, message) do
    raise Ecto.QueryError, query: query, message: message
  end

  defp field({{:., _, [{:&, _, [0]}, pk]}, _, []}, pk), do: :_id
  defp field({{:., _, [{:&, _, [0]}, field]}, _, []}, _), do: field

  binary_ops =
    [>: :"$gt", >=: :"$gte", <: :"$lt", <=: :"$lte", !=: :"$ne", in: :"$in"]
  bool_ops =
    [and: :"$and", or: :"$or"]

  @binary_ops Keyword.keys(binary_ops)
  @bool_ops Keyword.keys(bool_ops)

  Enum.map(binary_ops, fn {op, mongo_op} ->
    defp binary_op(unquote(op)), do: unquote(mongo_op)
  end)

  Enum.map(bool_ops, fn {op, mongo_op} ->
    defp bool_op(unquote(op)), do: unquote(mongo_op)
  end)

  defp mapped_pair_or_value({op, _, _} = tuple, params, pk) when is_atom(op) and op != :^ do
    {key, value} = pair(tuple, params, pk)
    Map.put(%{}, key, value)
  end
  defp mapped_pair_or_value(value, params, _pk) do
    encode_value(value, params)
  end

  defp pair({:fragment, _, _}, _, _) do
    raise ArgumentError, "Mongodb adapter does not support SQL fragment syntax."
  end
  defp pair({op, _, args}, params, pk) when op in @bool_ops do
    args = Enum.map(args, &mapped_pair_or_value(&1, params, pk))
    {bool_op(op), args}
  end
  defp pair({:in, _, [left, {:^, _, [ix, len]}]}, params, pk) do
    args =
      ix..ix+len-1
      |> Enum.map(&elem(params, &1))
      |> Enum.map(&encode_value(&1, params))

    {field(left, pk), %{"$in": args}}
  end
  defp pair({:is_nil, _, [expr]}, _, pk) do
    {field(expr, pk), nil}
  end
  defp pair({:==, _, [left, right]}, params, pk) do
    {field(left, pk), encode_value(right, params)}
  end
  defp pair({op, _, [left, right]}, params, pk) when op in @binary_ops do
    {field(left, pk), Map.put(%{}, binary_op(op), encode_value(right, params))}
  end
  defp pair({:not, _, [{:in, _, [left, {:^, _, [ix, len]}]}]}, params, pk) do
    args =
      ix..ix+len-1
      |> Enum.map(&elem(params, &1))
      |> Enum.map(&encode_value(&1, params))

    {field(left, pk), %{"$nin": args}}
  end
  defp pair({:not, _, [{:in, _, [left, right]}]}, params, pk) do
    {field(left, pk), %{"$nin": encode_value(right, params)}}
  end
  defp pair({:not, _, [{:is_nil, _, [expr]}]}, _, pk) do
    {field(expr, pk), %{"$neq": nil}}
  end
  defp pair({:not, _, [expr]}, params, pk) do
    {key, value} = pair(expr, params, pk)
    {:"$not", Map.put(%{}, key, value)}
  end
end
