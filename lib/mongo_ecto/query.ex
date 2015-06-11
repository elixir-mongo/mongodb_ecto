defmodule Mongo.Ecto.Query do
  @moduledoc false

  alias Ecto.Query.QueryExpr
  alias Ecto.Query.Tagged

  def all(query, params) do
    check_query(query)

    params = List.to_tuple(params)
    {collection, model} = query.from
    pk = primary_key(model)

    skip = offset_limit(query.offset)
    return = offset_limit(query.limit)
    projection = select(query.select.fields, model, pk, [])
    selector = wheres(query.wheres, params, pk)
    fields = fields(query.select.fields, pk, params)

    if query.order_bys != [] do
      orderby = order_bys(query.order_bys, params, pk)
      selector = %{"$query": selector, "$orderby": orderby}
    end

    {collection, selector, projection, skip, return, fields, pk}
  end

  def delete_all(query, params) do
    check_query(query)

    params = List.to_tuple(params)
    {collection, model} = query.from
    pk = primary_key(model)

    selector = wheres(query.wheres, params, pk)

    {collection, selector}
  end

  def delete(source, filter, pk) do
    selector = encode_document(filter, pk)

    {source, selector}
  end

  def update_all(query, fields, params) do
    check_query(query)

    params  = List.to_tuple(params)
    {collection, model} = query.from
    pk = primary_key(model)

    selector = wheres(query.wheres, params, pk)
    command = encode_document(fields, pk, params)

    {collection, selector, %{"$set": command}}
  end

  def insert(source, params, pk) do
    document = encode_document(params, pk)

    {source, document}
  end

  def update(source, fields, filter, pk) do
    selector = encode_document(filter, pk)
    command  = encode_document(fields, pk)

    {source, selector, %{"$set": command}}
  end

  def decode_document(document, pk) do
    Enum.into(document, %{}, fn
      {"_id", value} -> {Atom.to_string(pk), extract_value(value)}
      {key, value}   -> {key, extract_value(value)}
    end)
  end

  defp encode_document(document, pk, params \\ {}) do
    Enum.into(document, %{}, fn
      {key, value} when key == pk -> {:_id, value(value, params, pk)}
      {key, value} -> {key, value(value, params, pk)}
    end)
  end

  defp primary_key(nil), do: nil
  defp primary_key(model) do
    case model.__schema__(:primary_key) do
      [pk] -> pk
      _    -> :id
    end
  end

  defp check_query(query) do
    check(query.joins, [], query, "MongoDB adapter does not support join clauses")
    check(query.distinct, nil, query, "MongoDB adapter does not support distinct clauses")
    check(query.lock, nil, query, "MongoDB adapter does not support locking")

    # TODO we could support some of those with aggregation pipelines
    check(query.group_bys, [], query, "MongoDB adapter does not support group_by clauses")
    check(query.havings, [], query, "MongoDB adapter does not support having clauses")
  end

  defp check(expr, expr, _, _), do: nil
  defp check(_, _, query, message) do
    raise Ecto.QueryError, query: query, message: message
  end

  defp offset_limit(nil), do: 0
  defp offset_limit(%QueryExpr{expr: int}) when is_integer(int), do: int

  defp select([{:&, _, [0]}], _model, _pk, []) do
    %{}
  end
  defp select([], _model, pk, fields) do
    Enum.into(fields, %{}, fn
      field when field == pk -> {:_id, true}
      field -> {field, true}
    end)
  end
  defp select([{:&, _, [0]} | rest], model, pk, fields) do
    select(rest, model, pk, model.__schema__(:fields) ++ fields)
  end
  defp select([{{:., _, [{:&, _, [0]}, field]}, _, []} | rest], model, pk, fields) do
    select(rest, model, pk, [field | fields])
  end
  # FIXME can we do something better than simply pass on the values here, and
  #       then insert them when processing the query result?
  defp select([_ | rest], model, pk, fields) do
    select(rest, model, pk, fields)
  end

  defp fields(fields, pk, params) do
    Enum.map(fields, fn
      %Tagged{value: {:^, _, [idx]}} ->
        params |> elem(idx) |> value(params, pk) |> extract_value
      value ->
        value
    end)
  end

  # TODO with two clauses on the same field we should join them with an '$and'
  defp wheres(query, params, pk) do
    query
    |> Enum.into(%{}, fn %QueryExpr{expr: expr} -> pair(expr, params, pk) end)
  end

  defp order_bys(order_bys, params, pk) do
    order_bys
    |> Enum.flat_map(fn
      %QueryExpr{expr: expr} ->
        Enum.map(expr, &order_by_expr(&1, params, pk))
    end)
    |> Enum.into(%{})
  end

  defp order_by_expr({:asc,  expr}, params, pk), do: {value(expr, params, pk),  1}
  defp order_by_expr({:desc, expr}, params, pk), do: {value(expr, params, pk), -1}

  defp extract_value(int) when is_integer(int), do: int
  defp extract_value(atom) when is_atom(atom), do: atom
  defp extract_value(float) when is_float(float), do: float
  defp extract_value(string) when is_binary(string), do: string
  defp extract_value(list) when is_list(list), do: Enum.map(list, &extract_value/1)
  defp extract_value(%BSON.Binary{binary: value}), do: value
  defp extract_value(%BSON.ObjectId{value: value}), do: value
  defp extract_value(%BSON.DateTime{utc: utc}) do
    seconds = div(utc, 1000)
    usec = rem(utc, 1000) * 1000
    {date, {hour, min, sec}} = :calendar.gregorian_seconds_to_datetime(seconds)
    {date, {hour, min, sec, usec}}
  end

  defp value(int, _, _) when is_integer(int), do: int
  defp value(float, _, _) when is_float(float), do: float
  defp value(string, _, _) when is_binary(string), do: string
  defp value(atom, _, _) when is_atom(atom), do: atom
  defp value(list, params, pk) when is_list(list), do: Enum.map(list, &value(&1, params, pk))
  defp value({:^, _, [idx]}, params, pk), do: elem(params, idx) |> value(params, pk)
  defp value({{:., _, [{:&, _, [0]}, pk]}, _, []}, _, pk), do: :_id
  defp value({{:., _, [{:&, _, [0]}, field]}, _, []}, _, _) when is_atom(field), do: field
  defp value(%BSON.ObjectId{} = objectid, _, _), do: objectid
  defp value(%Tagged{value: value, type: type}, _, _), do: typed_value(value, type)
  defp value({{_, _, _} = date, {hour, min, sec, usec}}, _, _) do
    seconds = :calendar.datetime_to_gregorian_seconds({date, {hour, min, sec}})
    %BSON.DateTime{utc: seconds * 1000 + div(usec, 1000)}
  end

  defp typed_value(nil, _), do: nil
  defp typed_value(value, {:array, type}), do: Enum.map(value, &typed_value(&1, type))
  defp typed_value(value, :binary), do: %BSON.Binary{binary: value}
  defp typed_value(value, :uuid), do: %BSON.Binary{binary: value, subtype: :uuid}
  defp typed_value(value, :object_id), do: %BSON.ObjectId{value: value}

  binary_ops =
    [>: :"$gt", >=: :"$gte", <: :"$lt", <=: :"$lte", !=: :"$ne", in: :"$in"]
  bool_ops =
    [and: :"$and", or: :"$or"]

  @binary_ops Keyword.keys(binary_ops)
  @bool_ops Keyword.keys(bool_ops)

  Enum.map(binary_ops ++ bool_ops, fn {op, mongo_op} ->
    defp translate(unquote(op)), do: unquote(mongo_op)
  end)

  defp mapped_pair_or_value({op, _, _} = tuple, params, pk) when is_atom(op) and op != :^ do
    {key, value} = pair(tuple, params, pk)
    Map.put(%{}, key, value)
  end
  defp mapped_pair_or_value(value, params, pk) do
    value(value, params, pk)
  end

  defp pair({:fragment, _, _}, _, _) do
    raise ArgumentError, "Mongodb adapter does not support SQL fragment syntax."
  end
  defp pair({op, _, args}, params, pk) when op in @bool_ops do
    args = Enum.map(args, &mapped_pair_or_value(&1, params, pk))
    {translate(op), args}
  end
  defp pair({:in, _, [left, {:^, _, [ix, len]}]}, params, pk) do
    args = Enum.map(ix..ix+len-1, &elem(params, &1)) |> Enum.map(&value(&1, params, pk))
    {value(left, params, pk), %{"$in": args}}
  end
  defp pair({:is_nil, _, [expr]}, params, pk) do
    {value(expr, params, pk), nil}
  end
  defp pair({:==, _, [left, right]}, params, pk) do
    {value(left, params, pk), value(right, params, pk)}
  end
  defp pair({op, _, [left, right]}, params, pk) when op in @binary_ops do
    {value(left, params, pk), Map.put(%{}, translate(op), value(right, params, pk))}
  end
  defp pair({:not, _, [{:in, _, [left, {:^, _, [ix, len]}]}]}, params, pk) do
    args = Enum.map(ix..ix+len-1, &elem(params, &1)) |> Enum.map(&value(&1, params, pk))
    {value(left, params, pk), %{"$nin": args}}
  end
  defp pair({:not, _, [{:in, _, [left, right]}]}, params, pk) do
    {value(left, params, pk), %{"$nin": value(right, params, pk)}}
  end
  defp pair({:not, _, [{:is_nil, _, [expr]}]}, params, pk) do
    {value(expr, params, pk), %{"$neq": nil}}
  end
  defp pair({:not, _, [expr]}, params, pk) do
    {key, value} = pair(expr, params, pk)
    {:"$not", Map.put(%{}, key, value)}
  end
end
