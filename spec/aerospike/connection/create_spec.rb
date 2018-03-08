# frozen_string_literal: true

describe Aerospike::Connection::Create do
  let(:host) { 'thehost' }
  let(:port) { 3000 }
  let(:socket) { spy }
  let(:timeout) { 1 }

  describe '::call' do
    before do
      allow(::Aerospike::Socket::SSL).to receive(:new).and_return(socket)
      allow(::Aerospike::Socket::TCP).to receive(:new).and_return(socket)
      described_class.call(host, port, timeout, ssl_options)
    end

    shared_examples_for 'a tcp socket' do
      it do
        expect(
          ::Aerospike::Socket::TCP
        ).to have_received(:new).with(host, port, timeout)
      end
      it { expect(::Aerospike::Socket::SSL).not_to have_received(:new) }
      it { expect(socket).to have_received(:connect) }
    end

    context 'when ssl options indicates enabled' do
      let(:ssl_options) { { enable: true } }

      it do
        expect(
          ::Aerospike::Socket::SSL
        ).to have_received(:new).with(host, port, timeout, ssl_options)
      end

      it { expect(::Aerospike::Socket::TCP).not_to have_received(:new) }
      it { expect(socket).to have_received(:connect) }
    end

    context 'when ssl options are nil' do
      let(:ssl_options) { nil }

      it_behaves_like 'a tcp socket'
    end

    context 'when ssl options are empty' do
      let(:ssl_options) { {} }

      it_behaves_like 'a tcp socket'
    end
  end
end
