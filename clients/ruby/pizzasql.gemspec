Gem::Specification.new do |spec|
  spec.name          = "pizzasql"
  spec.version       = "0.1.0"
  spec.authors       = ["PizzaSQL"]
  spec.email         = ["hello@pizzasql.com"]

  spec.summary       = "Ruby client for PizzaSQL"
  spec.description   = "A simple, lightweight client library for PizzaSQL - a SQL database with HTTP API"
  spec.homepage      = "https://github.com/pizzasql/pizzasql"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 2.7.0"

  spec.files = Dir["lib/**/*", "README.md", "LICENSE"]
  spec.require_paths = ["lib"]

  # No external dependencies - uses only Ruby standard library
end
