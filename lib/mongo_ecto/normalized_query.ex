defmodule Mongo.Ecto.NormalizedQuery do
  @moduledoc false

  defmodule ReadQuery do
    @moduledoc false

    defstruct coll: nil, pk: nil, params: {}, query: %{}, projection: %{},
              fields: [], opts: []
  end

  defmodule WriteQuery do
    @moduledoc false

    defstruct coll: nil, query: %{}, command: %{}, opts: []
  end

  defmodule CommandQuery do
    @moduledoc false

    defstruct command: nil, database: nil, opts: []
  end

  alias Mongo.Ecto.Encoder
  alias Ecto.Query.Tagged
  alias Ecto.Query

  defmacrop is_op(op) do
    quote do
      is_atom(unquote(op)) and unquote(op) != :^
    end
  end

  def all(%Query{} = original, params) do
    check_query(original)

    from   = from(original)
    params = List.to_tuple(params)
    query  = query_order(original, params, from)

    case projection(original, params, from) do
      :count ->
        count(original, query, from)
      {projection, fields} ->
        find_all(original, query, projection, fields, params, from)
    end
  end

  defp find_all(original, query, projection, fields, params, {coll, _, pk}) do
    opts   = opts(:find_all, original)

    %ReadQuery{coll: coll, pk: pk, params: params, query: query, projection: projection,
               fields: fields, opts: opts}
  end

  defp count(original, query, {coll, _, _}) do
    command =
      [count: coll, query: query]
      |> put_if_not_zero(:limit, num_return(original))
      |> put_if_not_zero(:skip, num_skip(original))

    %CommandQuery{command: command, opts: opts(:command)}
  end

  def update_all(%Query{} = original, params) do
    check_query(original)

    params  = List.to_tuple(params)
    from    = from(original)
    coll    = coll(from)
    query   = query(original, params, from)
    command = command(:update, original, params, from)
    opts    = opts(:update_all, original)

    %WriteQuery{coll: coll, query: query, command: command, opts: opts}
  end

  def update({coll, _model}, values, filter, pk) do
    command = command(:update, values, pk)
    query   = query(filter, pk)

    %WriteQuery{coll: coll, query: query, command: command}
  end

  def delete_all(%Query{} = original, params) do
    check_query(original)

    params = List.to_tuple(params)
    from   = from(original)
    coll   = coll(from)
    query  = query(original, params, from)
    opts   = opts(:delete_all, original)

    %WriteQuery{coll: coll, query: query, opts: opts}
  end

  def delete({coll, _model}, filter, pk) do
    query = query(filter, pk)

    %WriteQuery{coll: coll, query: query}
  end

  def insert({coll, model}, document, pk) do
    command = command(:insert, document, model.__struct__(), pk)

    %WriteQuery{coll: coll, command: command}
  end

  def command(command, opts) do
    db = Keyword.get(opts, :database, nil)

    %CommandQuery{command: command, database: db, opts: opts(:command)}
  end

  defp from(%Query{from: {coll, model}}) do
    {coll, model, primary_key(model)}
  end

  defp query_order(original, params, from) do
    query = query(original, params, from)
    order = order(original, from)
    query_order(query, order)
  end

  defp query_order(query, order) when order == %{},
    do: query
  defp query_order(query, order),
    do: ["$query": query, "$orderby": order]

  defp projection(%Query{select: nil}, _params, _from),
    do: {%{}, []}
  defp projection(%Query{select: %Query.SelectExpr{fields: fields}} = query, params, from),
    do: projection(fields, params, from, query, %{}, [])

  defp projection([], _params, _from, _query, pacc, facc),
    do: {pacc, Enum.reverse(facc)}
  defp projection([{:&, _, [0]} = field | rest], params, {_, model, pk} = from, query, pacc, facc)
      when  model != nil do
    pacc = Enum.into(model.__schema__(:fields), pacc, &{field(&1, pk), true})
    facc = [field | facc]

    projection(rest, params, from, query, pacc, facc)
  end
  defp projection([{:&, _, [0]} = field | rest], params, {_, nil, _} = from, query, _pacc, facc) do
    # Model is nil, we want empty projection, but still extract fields
    {_, facc} = projection(rest, params, from, query, %{}, [field | facc])
    {%{}, facc}
  end
  defp projection([{{:., _, [_, name]}, _, _} = field| rest], params, from, query, pacc, facc) do
    {_, _, pk} = from

    # Projections use names as in database, fields as in models
    pacc = Map.put(pacc, field(name, pk), true)
    facc = [{:field, name, field} | facc]
    projection(rest, params, from, query, pacc, facc)
  end
  # Keyword and interpolated fragments
  defp projection([{:fragment, _, [args]} = field | rest], params, from, query, pacc, facc)
      when is_list(args) or tuple_size(args) == 3 do
    {_, _, pk} = from
    pacc =
      args
      |> value(params, pk, query, "select clause")
      |> Enum.into(pacc)
    facc = [field | facc]

    projection(rest, params, from, query, pacc, facc)
  end
  defp projection([{:count, _, _}], _params, _from, _query, pacc, _facc) when pacc == %{} do
    :count
  end
  defp projection([{:count, _, _}], _params, _from, query, _pacc, _facc) do
    error(query, "select clause (only one count without other selects is allowed)")
  end
  defp projection([{op, _, _} | _rest], _params, _from, query, _pacc, _facc) when is_op(op) do
    error(query, "select clause")
  end
  # We skip all values and then add them when constructing return result
  defp projection([%Tagged{value: {:^, _, [idx]}} = field | rest], params, from, query, pacc, facc) do
    {_, _, pk} = from
    value = params |> elem(idx) |> value(params, pk, query, "select clause")
    facc = [{:value, value, field} | facc]

    projection(rest, params, from, query, pacc, facc)
  end
  defp projection([field | rest], params, from, query, pacc, facc) do
    {_, _, pk} = from
    value = value(field, params, pk, query, "select clause")
    facc = [{:value, value, field} | facc]

    projection(rest, params, from, query, pacc, facc)
  end

  defp opts(:find_all, query),
    do: [exhaust: true, num_return: num_return(query), num_skip: num_skip(query)]
  defp opts(:update_all, _query),
    do: [multi: true]
  defp opts(:delete_all, _query),
    do: [multi: true]
  defp opts(:command),
    do: [num_return: -1, exhaust: true]

  defp put_if_not_zero(keyword, _key, 0),
    do: keyword
  defp put_if_not_zero(keyword, key, value),
    do: Keyword.put(keyword, key, value)

  defp num_skip(%Query{offset: offset}), do: offset_limit(offset)

  defp num_return(%Query{limit: limit}), do: offset_limit(limit)

  defp coll({coll, _model, _pk}), do: coll

  defp query(%Query{wheres: wheres} = query, params, {_coll, _model, pk}) do
    wheres
    |> Enum.map(fn %Query.QueryExpr{expr: expr} ->
      pair(expr, params, pk, query, "where clause")
    end)
    |> :lists.flatten
    |> merge_keys(query, "where clause")
  end

  defp query(filter, pk) do
    filter |> value(pk, "where clause") |> map_unless_empty
  end

  defp order(%Query{order_bys: order_bys} = query, {_coll, _model, pk}) do
    order_bys
    |> Enum.flat_map(fn %Query.QueryExpr{expr: expr} ->
      Enum.map(expr, &order_by_expr(&1, pk, query))
    end)
    |> map_unless_empty
  end

  defp command(:update, %Query{updates: updates} = query, params, {_coll, _model, pk}) do
    updates
    |> Enum.flat_map(fn %Query.QueryExpr{expr: expr} ->
      Enum.map(expr, fn {key, value} ->
        value = value |> value(params, pk, query, "update clause")
        {update_op(key, query), value}
      end)
    end)
    |> merge_keys(query, "update clause")
  end

  defp command(:insert, document, struct, pk) do
    document
    |> Enum.reject(fn {key, value} -> both_nil(value, Map.get(struct, key)) end)
    |> value(pk, "insert command") |> map_unless_empty
  end

  defp command(:update, values, pk) do
    ["$set": values |> value(pk, "update command") |> map_unless_empty]
  end

  defp both_nil(nil, nil), do: true
  defp both_nil(_, _), do: false

  defp offset_limit(nil),
    do: 0
  defp offset_limit(%Query.QueryExpr{expr: int}) when is_integer(int),
    do: int

  defp primary_key(nil),
    do: nil
  defp primary_key(model) do
    case model.__schema__(:primary_key) do
      []   -> nil
      [pk] -> pk
      keys ->
        raise ArgumentError, "MongoDB adapter does not support multiple primary keys " <>
          "and #{inspect keys} were defined in #{inspect model}."
    end
  end

  defp order_by_expr({:asc,  expr}, pk, query),
    do: {field(expr, pk, query, "order clause"),  1}
  defp order_by_expr({:desc, expr}, pk, query),
    do: {field(expr, pk, query, "order clause"), -1}

  defp check_query(query) do
    check(query.distinct, nil, query, "MongoDB adapter does not support distinct clauses")
    check(query.lock,     nil, query, "MongoDB adapter does not support locking")
    check(query.joins,     [], query, "MongoDB adapter does not support join clauses")
    check(query.group_bys, [], query, "MongoDB adapter does not support group_by clauses")
    check(query.havings,   [], query, "MongoDB adapter does not support having clauses")
  end

  defp check(expr, expr, _, _),
    do: nil
  defp check(_, _, query, message),
    do: raise(Ecto.QueryError, query: query, message: message)

  defp value(expr, pk, place) do
    case Encoder.encode(expr, pk) do
      {:ok, value} -> value
      :error       -> error(place)
    end
  end

  defp value(expr, params, pk, query, place) do
    case Encoder.encode(expr, params, pk) do
      {:ok, value} -> value
      :error       -> error(query, place)
    end
  end

  defp field(pk, pk), do: :_id
  defp field(key, _), do: key

  defp field({{:., _, [{:&, _, [0]}, field]}, _, []}, pk, _query, _place),
    do: field(field, pk)
  defp field(_expr, _pk, query, place),
    do: error(query, place)

  defp map_unless_empty([]), do: %{}
  defp map_unless_empty(list), do: list

  defp merge_keys(keyword, query, place) do
    Enum.reduce(keyword, %{}, fn {key, value}, acc ->
      Map.update(acc, key, value, fn
        old when is_list(old) -> old ++ value
        _                     -> error(query, place)
      end)
    end)
  end

  # For now we support only set & inc, we should support more in the future
  update = [set: :"$set", inc: :"$inc"]

  Enum.map(update, fn {key, op} ->
    def update_op(unquote(key), _query), do: unquote(op)
  end)

  def update_op(_, query), do: error(query, "update clause")

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

  defp mapped_pair_or_value({op, _, _} = tuple, params, pk, query, place) when is_op(op) do
    [pair(tuple, params, pk, query, place)]
  end
  defp mapped_pair_or_value(value, params, pk, query, place) do
    value(value, params, pk, query, place)
  end

  defp pair({op, _, args}, params, pk, query, place) when op in @bool_ops do
    args = Enum.map(args, &mapped_pair_or_value(&1, params, pk, query, place))
    {bool_op(op), args}
  end
  defp pair({:is_nil, _, [expr]}, _, pk, query, place) do
    {field(expr, pk, query, place), nil}
  end
  defp pair({:==, _, [left, right]}, params, pk, query, place) do
    {field(left, pk, query, place), value(right, params, pk, query, place)}
  end
  defp pair({:in, _, [left, {:^, _, [ix, len]}]}, params, pk, query, place) do
    args =
      ix..ix+len-1
      |> Enum.map(&elem(params, &1))
      |> Enum.map(&value(&1, params, pk, query, place))

    {field(left, pk, query, place), ["$in": args]}
  end
  defp pair({:in, _, [lhs, {{:., _, _}, _, _} = rhs]}, params, pk, query, place) do
    {field(rhs, pk, query, place), value(lhs, params, pk, query, place)}
  end
  defp pair({op, _, [left, right]}, params, pk, query, place) when op in @binary_ops do
    {field(left, pk, query, place), [{binary_op(op), value(right, params, pk, query, place)}]}
  end
  defp pair({:not, _, [{:in, _, [left, {:^, _, [ix, len]}]}]}, params, pk, query, place) do
    args =
      ix..ix+len-1
      |> Enum.map(&elem(params, &1))
      |> Enum.map(&value(&1, params, pk, query, place))

    {field(left, pk, query, place), ["$nin": args]}
  end
  defp pair({:not, _, [{:in, _, [left, right]}]}, params, pk, query, place) do
    {field(left, pk, query, place), ["$nin": value(right, params, pk, query, place)]}
  end
  defp pair({:not, _, [{:is_nil, _, [expr]}]}, _, pk, query, place) do
    {field(expr, pk, query, place), ["$ne": nil]}
  end
  defp pair({:not, _, [{:==, _, [left, right]}]}, params, pk, query, place) do
    {field(left, pk, query, place), ["$ne": value(right, params, pk, query, place)]}
  end
  defp pair({:not, _, [expr]}, params, pk, query, place) do
    {:"$not", [pair(expr, params, pk, query, place)]}
  end
  defp pair({:^, _, _} = expr, params, pk, query, place) do
    case value(expr, params, pk, query, place) do
      %BSON.JavaScript{} = js ->
        {:"$where", js}
      _value ->
        error(query, place)
    end
  end
  # Keyword or embedded fragment
  defp pair({:fragment, _, [args]}, params, pk, query, place)
      when is_list(args) or tuple_size(args) == 3 do
    value(args, params, pk, query, place)
  end
  defp pair(_expr, _params, _pk, query, place) do
    error(query, place)
  end

  defp error(query, place) do
    raise Ecto.QueryError, query: query,
      message: "Invalid expression for MongoDB adapter in #{place}"
  end
  defp error(place) do
    raise ArgumentError, "Invalid expression for MongoDB adapter in #{place}"
  end
end
