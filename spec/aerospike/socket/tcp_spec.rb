# frozen_string_literal: true

describe Aerospike::Socket::TCP do
  let(:instance) { described_class.new(host, port, timeout) }

  let(:host) { 'thehost' }
  let(:port) { 3000 }
  let(:timeout) { 1 }
  let(:socket) { double }

  before do
    allow(::Socket).to receive(:new).and_return(socket)
    allow(::Socket).to receive(:sockaddr_in)
  end

  describe '#initalize' do
    before { described_class.new(host, port, timeout) }

    it do
      expect(::Socket).to have_received(:new).with(::Socket::AF_INET, ::Socket::SOCK_STREAM, 0)
    end
  end

  describe '#family' do
    subject { instance.family }

    it { is_expected.to eq ::Socket::AF_INET }
  end

  describe '#connect!' do
    before do
      allow(socket).to receive(:connect_nonblock)
      instance.connect!
    end

    it { expect(socket).to have_received(:connect_nonblock) }
  end
end