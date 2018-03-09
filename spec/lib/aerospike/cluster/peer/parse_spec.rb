# frozen_string_literal: true

RSpec.describe Aerospike::Cluster::Peer::Parse do
  describe "::call" do
    subject(:parsed) { described_class.call(response) }
    subject(:first_peer) { parsed.peers.first }

    context 'when empty response' do
      let(:response) { '1,,[]' }

      it { expect(parsed.generation).to eq 1 }
      it { expect(parsed.port_default).to be_nil }
      it { expect(parsed.peers).to be_empty }
    end

    context 'with tls names' do
      let(:response) do
        '3,3144,[[C1D4DC08D270008,aerospike,[192.168.33.10]],[C814DC08D270008,aerospike,[192.168.33.10:3244]]]'
      end

      it { expect(parsed.generation).to eq 3 }
      it { expect(parsed.port_default).to eq 3144 }
      it { expect(parsed.peers.size).to eq 2 }
      it { expect(first_peer.node_name).to eq 'C1D4DC08D270008' }
      it { expect(first_peer.tls_name).to eq 'aerospike' }
      it { expect(first_peer.hosts.size).to eq 1 }
    end

    context 'with invalid response' do
      let(:response) { ',,' }

      it { expect { parsed }.to raise_error(::Aerospike::Exceptions::Parse) }
    end
  end
end
