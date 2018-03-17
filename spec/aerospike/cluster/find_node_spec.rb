# frozen_string_literal: true

RSpec.describe Aerospike::Cluster::FindNode do
  let(:cluster) { double }
  let(:peers) { double }
  let(:node_name) { 'node' }

  describe '::call' do
    before do
      allow(cluster).to receive(:find_node_by_name).and_return(cluster_node)
      allow(peers).to receive(:find_node_by_name).and_return(peer_node)
    end

    subject(:find_node) { described_class.call(cluster, peers, node_name) }

    context 'when node is found in cluster' do
      let(:cluster_node) { spy }
      let(:peer_node) { spy }

      before { find_node }

      it { is_expected.to be cluster_node }
      it { expect(cluster_node).to have_received(:increase_reference_count!)  }
    end

    context 'when node is not found in cluster but in peers' do
      let(:cluster_node) { nil }
      let(:peer_node) { spy }

      before { find_node }

      it { is_expected.to be peer_node }
      it { expect(peer_node).to have_received(:increase_reference_count!)  }
    end

    context 'when node is not found' do
      let(:cluster_node) { nil }
      let(:peer_node) { nil }

      before { find_node }

      it { is_expected.to be_nil }
    end
  end
end
