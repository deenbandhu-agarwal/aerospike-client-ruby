# frozen_string_literal: true

RSpec.describe Aerospike::Host::Parse do
  describe "::call" do
    subject(:parsed) { described_class.(str, default_port) }

    let(:default_port) { 3000 }
    let(:first_item) { parsed.first }

    context 'with single tls name' do
      let(:str) { '192.168.1.10:cert1:3000' }

      it { expect(parsed.size).to eq 1 }
      it { expect(first_item.name).to eq '192.168.1.10' }
      it { expect(first_item.tls_name).to eq 'cert1' }
      it { expect(first_item.port).to eq 3000 }
    end

    context 'with two names' do
      let(:str) { 'host1:3000,host2:3000' }

      it { expect(parsed.size).to eq 2 }
      it { expect(first_item.name).to eq 'host1' }
      it { expect(first_item.tls_name).to be_nil }
      it { expect(first_item.port).to eq 3000 }
    end
  end
end