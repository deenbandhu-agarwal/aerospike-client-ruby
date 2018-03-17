# frozen_string_literal: true

RSpec.describe Aerospike::Cluster do
  let(:instance) { described_class.new(policy, hosts) }
  let(:policy) { spy }
  let(:hosts) { [] }

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
