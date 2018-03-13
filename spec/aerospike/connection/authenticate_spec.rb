# frozen_string_literal: true

RSpec.describe Aerospike::Connection::Authenticate do
  let(:command) { double }
  let(:conn) { double }
  let(:password) { 'password' }
  let(:user) { 'user' }

  before do
    allow(command).to receive(:authenticate)
    allow(conn).to receive(:close)
    allow(::Aerospike::AdminCommand).to receive(:new).and_return(command)
  end

  describe '::call' do
    subject(:authenticate) { described_class.call(conn, user, password) }

    context 'when authentication is successful' do
      it { is_expected.to eq true }
    end

    context 'when authentication fails' do
      before do
        allow(command).to receive(:authenticate).and_raise(::Aerospike::Exceptions::Aerospike.new(0))
      end

      it do
        expect { authenticate }.to raise_error(
          ::Aerospike::Exceptions::InvalidCredentials
        ) do |_|
          expect(conn).to have_received(:close)
        end
      end
    end
  end
end