# Simple

To run this example, you need to ensure mongodb is up and running with a
`mongodb` username and `mongodb` password. If you want to run with another
credentials, just change the settings in the `config/config.exs` file.

Then, from the command line:

* `mix do deps.get, compile`
* `iex -S mix`

Inside IEx, run:

* `Simple.sample_query`
