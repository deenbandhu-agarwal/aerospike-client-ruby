# frozen_string_literal: true

RSpec.describe Aerospike::Cluster::CreateConnection do
  let(:cluster) { spy }
  let(:host) { spy }

  describe '::call' do
    subject(:create_connection) { described_class.call(cluster, host) }

    before do
      allow(::Aerospike::Connection::Create).to receive(:call)
      allow(::Aerospike::Connection::Authenticate).to receive(:call)
      allow(cluster).to receive(:credentials_given?).and_return(authenticate)
    end

    context 'when user and password is given' do
      let(:authenticate) { true }

      before { create_connection }

      it { expect(::Aerospike::Connection::Authenticate).to have_received(:call) }
    end

    context 'when user and password is given' do
      let(:authenticate) { false }

      before { create_connection }

      it { expect(::Aerospike::Connection::Authenticate).not_to have_received(:call) }
    end
  end
end
