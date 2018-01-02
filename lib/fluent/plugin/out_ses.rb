require 'fluent/plugin/output'

module Fluent::Plugin
  class SESOutput < Output
    Fluent::Plugin.register_output('ses', self)

    def initialize
      super
      require 'aws-sdk-ses'
    end

    include Fluent::SetTagKeyMixin
    config_set_default :include_tag_key, false
    include Fluent::SetTimeKeyMixin

    config_param :aws_key_id,  :string
    config_param :aws_sec_key, :string
    config_param :aws_region,  :string

    config_param :from,               :string
    config_param :to,                 :string, :default => ""
    config_param :cc,                 :string, :default => ""
    config_param :bcc,                :string, :default => ""
    config_param :subject,            :string, :default => ""
    config_param :reply_to_addresses, :string, :default => ""

    def start
      super

      credentials = Aws::Credentials.new(@aws_key_id, @aws_sec_key)
      @ses = Aws::SES::Client.new(region: @aws_region, credentials: credentials)

      to_addresses  = @to.split ","
      if to_addresses.empty?
        raise Fluent::ConfigError, "To is not nil."
      end

      cc_addresses  = @cc.split ","
      bcc_addresses = @bcc.split ","

      @destination = {:to_addresses => to_addresses}
      unless cc_addresses.empty?
        @destination[:cc_addresses] = cc_addresses
      end
      unless bcc_addresses.empty?
        @destination[:bcc_addresses] = bcc_addresses
      end

      log.debug "ses: started"
    end

    # method for async buffered output mode
    def try_write(chunk)
      chunk_id = chunk.unique_id
      log.info "ses: try_write", chunk_id: dump_unique_id_hex(chunk_id)

      chunks = {}
      chunk.each do |time, record|
        time = time.to_r if time.is_a?(Fluent::EventTime)
        chunks[time] = Time.at(time).to_s

        record.each do |key, value|
          chunks[time] << "  - #{key}: #{value}\n"
        end

        chunks[time] << "\n"
      end

      body_text = begin
        chunks.sort.reverse.map(&:last).join
      rescue StandardError => e
        "Rescued #{e.inspect} with: #{chunks.inspect}"
      end

      body_text << <<-METADATA.gsub(/\A\s{8}/, "")
        Metadata:
          - timekey: #{chunk.metadata.timekey.inspect}
          - tag: #{chunk.metadata.tag.inspect}
          - variables: #{chunk.metadata.variables.inspect}
      METADATA

      options = {
        :source      => @from,
        :destination => @destination,
        :message => {
          :subject => {          :data => @subject},
          :body    => {:text => {:data => body_text}},
        },
      }

      reply_to_addresses = @reply_to_addresses.split ","
      unless reply_to_addresses.empty?
        options[:reply_to_addresses] = reply_to_addresses
      end

      begin
        @ses.send_email options
        log.info "ses: message sent"
        commit_write(chunk_id)
      rescue => e
        log.error "ses: #{e.message}"
      end
    end
  end
end
