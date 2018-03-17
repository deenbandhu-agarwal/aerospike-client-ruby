# frozen_string_literal: true

RSpec.describe Aerospike::Node::Generation do
  let(:instance) { described_class.new(0) }

  describe '#reset_changed!' do
    subject(:reset_changed) { instance.reset_changed! }

    context 'with updated value' do
      before { instance.update(1) }

      it { expect { reset_changed }.to change(instance, :changed?).from(true).to(false) }
    end

    context 'with same value' do
      before { instance.update(0) }

      it { expect { reset_changed }.not_to change(instance, :changed?) }
    end
  end

  describe '#update' do
    subject(:update) { instance.update(new_number) }

    context 'when setting new value' do
      let(:new_number) { 1 }

      it { expect { update }.to change(instance.number, :value).from(0).to(new_number) }

      it { expect { update }.to change(instance, :changed?).from(false).to(true) }
    end
  end
end
