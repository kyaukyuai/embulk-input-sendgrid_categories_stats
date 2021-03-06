
Gem::Specification.new do |spec|
  spec.name          = "embulk-input-sendgrid_categories_stats"
  spec.version       = "0.1.1"
  spec.authors       = ["kyaukyuai"]
  spec.summary       = "Sendgrid Categories Stats input plugin for Embulk"
  spec.description   = "Loads records from Sendgrid Categories Stats."
  spec.email         = ["y.kakui@gmail.com"]
  spec.licenses      = ["MIT"]
  # TODO set this: spec.homepage      = "https://github.com/y.kakui/embulk-input-sendgrid_categories_stats"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  #spec.add_dependency 'YOUR_GEM_DEPENDENCY', ['~> YOUR_GEM_DEPENDENCY_VERSION']
  spec.add_dependency 'rest-client', ['>= 1.8.0']
  spec.add_development_dependency 'embulk', ['>= 0.9.23']
  spec.add_development_dependency 'bundler', ['>= 1.10.6']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
