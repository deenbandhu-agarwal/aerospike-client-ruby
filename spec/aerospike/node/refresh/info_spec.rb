# frozen_string_literal: true

RSpec.describe Aerospike::Node::Refresh::Info do
  let(:node) { double }
  let(:peers) { ::Aerospike::Peers.new }
  let(:connection) { spy }

  before do
    allow(::Aerospike::Node::Verify::PeersGeneration).to receive(:call)
    allow(::Aerospike::Node::Verify::PartitionGeneration).to receive(:call)
    allow(::Aerospike::Node::Verify::ClusterName).to receive(:call)
    allow(::Aerospike::Node::Verify::Name).to receive(:call)
    allow(::Aerospike::Node::Refresh::Failed).to receive(:call)
    allow(::Aerospike::Node::Refresh::Friends).to receive(:call)
    allow(node).to receive(:decrease_health)
    allow(node).to receive(:restore_health)
    allow(node).to receive(:responded!)
    allow(node).to receive(:reset_failures!)
    allow(node).to receive(:get_connection).and_return(connection)
  end

  describe '::call' do
    subject(:call!) { described_class.call(node, peers) }

    context 'when using peers protocol' do
      before do
        allow(peers).to receive(:use_peers?).and_return(true)
        call!
      end

      it { expect(::Aerospike::Node::Verify::PeersGeneration).to have_received(:call) }
      it { expect(::Aerospike::Node::Verify::PartitionGeneration).to have_received(:call) }
      it { expect(::Aerospike::Node::Refresh::Friends).not_to have_received(:call) }
      it { expect(node).to have_received(:reset_failures!) }
    end

    context 'when not using peers protocol' do
      before do
        allow(peers).to receive(:use_peers?).and_return(false)
        call!
      end

      it { expect(::Aerospike::Node::Verify::PartitionGeneration).to have_received(:call) }
      it { expect(::Aerospike::Node::Refresh::Friends).to have_received(:call) }
      it { expect(node).to have_received(:reset_failures!) }
    end

    context 'when node name verification fails' do
      before do
        allow(::Aerospike::Node::Verify::Name).to receive(:call).and_raise(
          ::Aerospike::Exceptions::Aerospike.new(0, '')
        )
        call!
      end

      it { expect(connection).to have_received(:close) }
      it { expect(node).to have_received(:decrease_health) }
      it { expect(::Aerospike::Node::Refresh::Failed).to have_received(:call) }
    end
  end
end
