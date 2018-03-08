# frozen_string_literal: true

describe Aerospike::Socket::SSL do
  let(:context) { spy }
  let(:host) { 'thehost' }
  let(:instance) { described_class.new(host, port, timeout, ssl_options) }
  let(:port) { 3000 }
  let(:socket) { spy }
  let(:timeout) { 1 }
  let(:ssl_options) { { } }

  before do
    allow(::OpenSSL::SSL::SSLContext).to receive(:new).and_return(context)
    allow(::OpenSSL::SSL::SSLSocket).to receive(:new).and_return(socket)
  end

  describe '#initialize' do
    before { instance }

    context 'when a context is not passed in options' do
      let(:ssl_options) { {} }

      it { expect(::OpenSSL::SSL::SSLContext).to have_received(:new) }
    end

    context 'when a context is passed in options' do
      let(:ssl_options) { { context: context } }

      it { expect(::OpenSSL::SSL::SSLContext).not_to have_received(:new) }
    end
  end

  describe '#create_context' do
    before do
      %i[
          set_cert set_key set_cert_verification
          set_cipher_suite set_versions open_sockets
      ].each do |mthd|
        allow(instance).to receive(mthd)
      end

      instance.send('create_context', ssl_options)
    end

    it { expect(instance).to have_received(:set_cert).with(context, ssl_options) }
    it { expect(instance).to have_received(:set_key).with(context, ssl_options) }
    it { expect(instance).to have_received(:set_cert_verification).with(context, ssl_options) }
    it { expect(instance).to have_received(:set_versions).with(context, ssl_options) }
  end
end