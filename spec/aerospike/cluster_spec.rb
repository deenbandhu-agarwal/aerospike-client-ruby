# frozen_string_literal: true

RSpec.describe Aerospike::Cluster do
  let(:instance) { described_class.new(policy, hosts) }
  let(:policy) { spy }
  let(:hosts) { [] }

  describe '#refresh_nodes' do
    subject(:refresh_nodes) { instance.refresh_nodes }
    let!(:peers) { ::Aerospike::Peers.new }
    let(:node) { spy }
    let(:node_generation_changed) { false }
    let(:generation_changed) { false }
    let(:nodes_to_remove) { [] }
    let(:peer_nodes) { {} }

    before do
      allow(::Aerospike::Peers).to receive(:new).and_return(peers)
      allow(instance).to receive(:nodes).and_return(nodes)
      allow(instance).to receive(:add_nodes)
      allow(instance).to receive(:remove_nodes)
      allow(instance).to receive(:seed_nodes)
      allow(instance).to receive(:find_nodes_to_remove).and_return(nodes_to_remove)
      allow(::Aerospike::Node::Refresh::Info).to receive(:call)
      allow(::Aerospike::Node::Refresh::Peers).to receive(:call)
      allow(::Aerospike::Node::Refresh::Partitions).to receive(:call)
      allow(node.partition_generation).to receive(:changed?).and_return(node_generation_changed)
      allow(peers).to receive(:generation_changed?).and_return(generation_changed)
      peers.nodes = peer_nodes

      refresh_nodes
    end

    context 'with no nodes' do
      let(:nodes) { [] }

      it { expect(instance).to have_received(:seed_nodes) }
      it { is_expected.to be true }
    end

    context 'with two nodes' do
      let(:nodes) { [node, node] }

      it { expect(node).to have_received(:reset_reference_count!).twice }
      it { expect(node).to have_received(:reset_responded!).twice }

      context 'when peer generation has not changed' do
        let(:generation_changed) { false }

        it { expect(::Aerospike::Node::Refresh::Info).to have_received(:call).twice.with(node, peers) }
        it { expect(::Aerospike::Node::Refresh::Peers).not_to have_received(:call) }
        it { expect(::Aerospike::Node::Refresh::Partitions).not_to have_received(:call) }
        it { expect(instance).not_to have_received(:find_nodes_to_remove) }
      end

      context 'when peer generation has changed' do
        let(:generation_changed) { true }

        it { expect(::Aerospike::Node::Refresh::Info).to have_received(:call).twice.with(node, peers) }
        it { expect(::Aerospike::Node::Refresh::Peers).to have_received(:call).twice.with(node, peers) }
        it { expect(::Aerospike::Node::Refresh::Partitions).not_to have_received(:call)}
        it { expect(instance).to have_received(:find_nodes_to_remove).with(peers.refresh_count) }
      end

      context 'with nodes to remove' do
        let(:generation_changed) { true }
        let(:nodes_to_remove) { [node] }

        it { expect(instance).to have_received(:remove_nodes).with(nodes_to_remove) }
        it { is_expected.to be true }
      end

      context 'with nodes to add' do
        let(:peer_nodes) { { 'node1' => node} }

        it { expect(instance).to have_received(:add_nodes).with(peer_nodes.values) }
        it { is_expected.to be true }
      end
    end
  end

  describe '#tls_enabled?' do
    subject { instance.tls_enabled? }

    before { allow(instance).to receive(:ssl_options).and_return(ssl_options) }

    context 'when ssl_options enabled' do
      let(:ssl_options) { { enable: true } }

      it { is_expected.to be true }
    end

    context 'when ssl_options disabled' do
      let(:ssl_options) { { enable: false } }

      it { is_expected.to be false }
    end

    context 'when ssl_options is nil' do
      let(:ssl_options) { nil }

      it { is_expected.to be false }
    end
  end
end
