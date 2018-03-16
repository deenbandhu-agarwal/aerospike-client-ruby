source "https://rubygems.org"

group :test do
  gem 'rspec', '~> 3.4'
  gem 'codecov', require: false
end

group :development do
  gem 'rubocop', require: false
end

gem 'rake'
gem 'msgpack-jruby', :require => 'msgpack', :platforms => :jruby
gem 'msgpack', '~> 1.0', :platforms => [:mri, :rbx]
gem 'bcrypt'

platforms :mri, :rbx do
  install_if -> { Gem::Version.new(RUBY_VERSION) >= Gem::Version.new('2.3.0') } do
    gem 'openssl'
  end
end

platforms :jruby do
  gem 'jruby-openssl'
end

gemspec
