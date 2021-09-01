module Embulk
  module Input
    require 'json'
    require 'rest-client'

    class SendgridCategoriesStats < InputPlugin
      # input plugin file name must be: embulk/input/<name>.rb
      Plugin.register_input("sendgrid_categories_stats", self)

      def self.transaction(config, &control)
        task = {
          api_key: config.param(:api_key, :string),
          start_date: config.param(:start_date, :string, default: 'yesterday'),
          end_date: config.param(:end_date, :string, default: 'yesterday'),
          # TODO: assign multiple categories
          categories: config.param(:categories, :string)
        }

        columns = [
          Column.new(0, 'date', :timestamp),
          Column.new(1, 'category', :string),
          Column.new(2, 'blocks', :long),
          Column.new(3, 'bounce_drops', :long),
          Column.new(4, 'bounces', :long),
          Column.new(5, 'clicks', :long),
          Column.new(6, 'deferred', :long),
          Column.new(7, 'delivered', :long),
          Column.new(8, 'invalid_emails', :long),
          Column.new(9, 'opens', :long),
          Column.new(10, 'processed', :long),
          Column.new(11, 'requests', :long),
          Column.new(12, 'spam_report_drops', :long),
          Column.new(13, 'spam_reports', :long),
          Column.new(14, 'unique_clicks', :long),
          Column.new(15, 'unique_opens', :long),
          Column.new(16, 'unsubscribe_drops', :long),
          Column.new(17, 'unsubscribes', :long)
        ]

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        puts "Sendgrid input started."
        task_reports = yield(task, columns, count)
        puts "Sendgrid input finished. Commit reports = #{task_reports.to_json}"

        next_config_diff = {}
        return next_config_diff
      end

      def initialize(task, schema, index, page_builder)
        super
      end

      def run
        puts "Sendgrid input thread #{@index}..."

        start_date = @task[:start_date] == 'yesterday' ? Date.today - 1 : Date.parse(@task[:start_date])
        end_date   = @task[:end_date] == 'yesterday' ? Date.today - 1 : Date.parse(@task[:end_date])
        categories = @task[:categories]
        endpoint   = 'https://api.sendgrid.com/v3/categories/stats'

        json = RestClient.get(
          endpoint,
          {
            params: {
              start_date: start_date.strftime("%Y-%m-%d"),
              end_date: end_date.strftime("%Y-%m-%d"),
              categories: categories
            },
            Authorization: "Bearer #{@task[:api_key]}",
            content_type: "application/json"
          }
        )
        results = JSON.parse(json)

        results.each do |result|
          result["stats"].each do |metrics|
            @page_builder.add(metrics["metrics"].values.unshift(metrics["name"]).unshift(Time.parse(result["date"])))
          end
        end

        @page_builder.finish  # don't forget to call finish :-)
        return {}
      end
    end

  end
end
