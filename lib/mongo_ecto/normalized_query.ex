defmodule Mongo.Ecto.NormalizedQuery.Helper do
  @moduledoc false

  defmacro is_op(op) do
    quote do
      is_atom(unquote(op)) and unquote(op) != :^
    end
  end
end

defmodule Mongo.Ecto.NormalizedQuery do
  @moduledoc false

  defmodule QueryAll do
    @moduledoc false

    defstruct params: {}, from: {nil, nil, nil}, query: %{}, projection: %{},
              fields: [], opts: []
  end

  defmodule QueryUpdate do
    @moduledoc false

    defstruct coll: nil, query: %{}, command: %{}, opts: []
  end

  defmodule QueryDelete do
    @moduledoc false

    defstruct coll: nil, query: %{}, opts: []
  end

  defmodule QueryInsert do
    @moduledoc false

    defstruct coll: nil, command: %{}, opts: []
  end

  import Mongo.Ecto.NormalizedQuery.Helper
  alias Mongo.Ecto.Encoder
  alias Ecto.Query

  def all(%Query{} = original, params) do
    check_query(original)

    params     = List.to_tuple(params)
    from       = from(original)
    query      = query_order(original, params, from)
    projection = projection(original, from)
    fields     = fields(original, params)
    opts       = opts(:all, original)

    %QueryAll{params: params, from: from, query: query, projection: projection,
              fields: fields, opts: opts}
  end

  def update_all(%Query{} = original, values, params) do
    check_query(original)

    params  = List.to_tuple(params)
    from    = from(original)
    coll    = coll(from)
    query   = query(original, params, from)
    command = command(:update, query, values, params, from)
    opts    = opts(:update_all, original)

    %QueryUpdate{coll: coll, query: query, command: command, opts: opts}
  end

  def update(coll, values, filter, pk) do
    command = command(:update, values, pk)
    query   = query(filter, pk)

    %QueryUpdate{coll: coll, query: query, command: command}
  end

  def delete_all(%Query{} = original, params) do
    check_query(original)

    params = List.to_tuple(params)
    from   = from(original)
    coll   = coll(from)
    query  = query(original, params, from)
    opts   = opts(:delete_all, original)

    %QueryDelete{coll: coll, query: query, opts: opts}
  end

  def delete(coll, filter, pk) do
    query = query(filter, pk)

    %QueryDelete{coll: coll, query: query}
  end

  def insert(coll, document, pk) do
    command = command(:insert, document, pk)

    %QueryInsert{coll: coll, command: command}
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
    do: %{"$query": query, "$orderby": order}

  defp projection(%Query{select: nil}, _from),
    do: %{}
  defp projection(%Query{select: %Query.SelectExpr{fields: [{:&, _, [0]}]}}, _from),
    do: %{}
  defp projection(%Query{select: %Query.SelectExpr{fields: fields}} = query,
                  {_coll, model, pk}) do
    Enum.flat_map(fields, fn
      {:&, _, [0]} ->
        model.__schema__(:fields)
      {{:., _, [{:&, _, [0]}, field]}, _, []} when field == pk ->
        [:_id]
      {{:., _, [{:&, _, [0]}, field]}, _, []} ->
        [field]
      {op, _, _} = expr when is_op(op) ->
        error(:projection, query, expr)
      _value ->
        # We skip all values and then add them when constructing return result
        []
    end)
    |> Enum.map(&{&1, true})
  end

  defp fields(%Query{select: nil}, _params),
    do: []
  defp fields(%Query{select: %Query.SelectExpr{fields: fields}} = query, params) do
    Enum.map(fields, fn
      %Query.Tagged{value: {:^, _, [idx]}} ->
        params |> elem(idx) |> value(params, query)
      value ->
        value
    end)
  end

  defp opts(:all, query),
    do: [num_return: num_return(query), num_skip: num_skip(query)]
  defp opts(:update_all, _query),
    do: [multi: true]
  defp opts(:delete_all, _query),
    do: [multi: true]

  defp num_skip(%Query{offset: offset}), do: offset_limit(offset)

  defp num_return(%Query{limit: limit}), do: offset_limit(limit)

  defp coll({coll, _model, _pk}), do: coll

  defp query(%Query{wheres: wheres} = query, params, {_coll, _model, pk}) do
    Enum.into(wheres, %{}, fn %Query.QueryExpr{expr: expr} ->
      pair(expr, params, pk, query)
    end)
  end
  defp query(filter, pk) do
    case Encoder.encode_document(filter, pk) do
      {:ok, document} -> document
      {:error, expr}  ->
        error(:value, expr)
    end
  end

  defp order(%Query{order_bys: order_bys} = query, {_coll, _model, pk}) do
    order_bys
    |> Enum.flat_map(fn %Query.QueryExpr{expr: expr} -> expr end)
    |> Enum.into(%{}, &order_by_expr(&1, pk, query))
  end

  defp command(:update, query, values, params, {_coll, _model, pk}) do
    updates =
      case Encoder.encode_document(values, params, pk) do
        {:ok, document} -> Enum.into(document, %{})
        {:error, expr}  ->
          error(:value, query, expr)
      end

    %{"$set": updates}
  end
  defp command(:update, values, pk) do
    updates =
      case Encoder.encode_document(values, pk) do
        {:ok, document} -> Enum.into(document, %{})
        {:error, expr}  ->
          error(:value, expr)
      end

    %{"$set": updates}
  end
  defp command(:insert, document, pk) do
    case Encoder.encode_document(document, pk) do
      {:ok, document} -> document
      {:error, expr}  ->
        error(:value, expr)
    end
  end

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
    do: {field(expr, pk, query),  1}
  defp order_by_expr({:desc, expr}, pk, query),
    do: {field(expr, pk, query), -1}

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

  defp value(expr, params, query) do
    case Encoder.encode_value(expr, params) do
      {:ok, value}   -> value
      {:error, expr} -> error(:value, query, expr)
    end
  end

  defp field({{:., _, [{:&, _, [0]}, pk]}, _, []}, pk, _query),
    do: :_id
  defp field({{:., _, [{:&, _, [0]}, field]}, _, []}, _pk, _query),
    do: field
  defp field(expr, _pk, query),
    do: error(:field, query, expr)

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

  defp mapped_pair_or_value({op, _, _} = tuple, params, pk, query) when is_op(op) do
    {key, value} = pair(tuple, params, pk, query)
    Map.put(%{}, key, value)
  end
  defp mapped_pair_or_value(value, params, _pk, query) do
    value(value, params, query)
  end

  defp pair({op, _, args}, params, pk, query) when op in @bool_ops do
    args = Enum.map(args, &mapped_pair_or_value(&1, params, pk, query))
    {bool_op(op), args}
  end
  defp pair({:in, _, [left, {:^, _, [ix, len]}]}, params, pk, query) do
    args =
      ix..ix+len-1
      |> Enum.map(&elem(params, &1))
      |> Enum.map(&value(&1, params, query))

    {field(left, pk, query), %{"$in": args}}
  end
  defp pair({:is_nil, _, [expr]}, _, pk, query) do
    {field(expr, pk, query), nil}
  end
  defp pair({:==, _, [left, right]}, params, pk, query) do
    {field(left, pk, query), value(right, params, query)}
  end
  defp pair({op, _, [left, right]}, params, pk, query) when op in @binary_ops do
    {field(left, pk, query), Map.put(%{}, binary_op(op), value(right, params, query))}
  end
  defp pair({:not, _, [{:in, _, [left, {:^, _, [ix, len]}]}]}, params, pk, query) do
    args =
      ix..ix+len-1
      |> Enum.map(&elem(params, &1))
      |> Enum.map(&value(&1, params, query))

    {field(left, pk, query), %{"$nin": args}}
  end
  defp pair({:not, _, [{:in, _, [left, right]}]}, params, pk, query) do
    {field(left, pk, query), %{"$nin": value(right, params, query)}}
  end
  defp pair({:not, _, [{:is_nil, _, [expr]}]}, _, pk, query) do
    {field(expr, pk, query), %{"$neq": nil}}
  end
  defp pair({:not, _, [{:==, _, [left, right]}]}, params, pk, query) do
    {field(left, pk, query), %{"$neq": value(right, params, query)}}
  end
  defp pair({:not, _, [expr]}, params, pk, query) do
    {key, value} = pair(expr, params, pk, query)
    {:"$not", Map.put(%{}, key, value)}
  end
  defp pair({:^, _, _} = expr, params, _pk, query) do
    case value(expr, params, query) do
      %BSON.JavaScript{} = js ->
        {:"$where", js}
      _value ->
        error(:expression, query, expr)
    end
  end
  defp pair(expr, _params, _pk, query) do
    error(:expression, query, expr)
  end

  defp error(expected, given) do
    raise ArgumentError,
      "MongoDB adapter expected #{expected}, but `#{format_expr(given)}` was given"
  end
  defp error(expected, query, given) do
    raise Ecto.QueryError, query: query,
      message: "MongoDB adapter expected #{expected}, but `#{format_expr(given)}` was given"
  end

  defp format_expr({atom, _, _} = ast) when is_atom(atom),
    do: Macro.to_string(ast)
  defp format_expr(string) when is_binary(string),
    do: string
  defp format_expr(expr),
    do: inspect(expr)
end
