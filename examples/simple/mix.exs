defmodule Simple.Mixfile do
  use Mix.Project

  def project do
    [app: :simple,
     version: "0.0.1",
     deps: deps]
  end

  def application do
    [mod: {Simple.App, []},
     applications: [:mongodb_ecto, :ecto]]
  end

  defp deps do
    [{:mongodb_ecto, path: "../.."}]
  end
end
