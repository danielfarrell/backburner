module Backburner
  module Workers
    class Forking < Worker

      def prepare
        self.tube_names.map! { |name| expand_tube_name(name)  }
        log_info "Working #{tube_names.size} queues: [ #{tube_names.join(', ')} ]"
        self.connection.tubes.watch!(*self.tube_names)
      end

      def start
        prepare
        loop { work_one_job }
      end

      def work_one_job
        task = self.connection.tubes.reserve
        pid = Process.fork do
          task.connection.reconnect!
          work_one_task(task)
        end
        Process.wait(pid)
        if $?.exitstatus
          task.delete
        else
          num_retries = task.stats.releases
          retry_status = "failed: attempt #{num_retries+1} of #{config.max_job_retries+1}"
          if num_retries < config.max_job_retries # retry again
            delay = config.retry_delay + num_retries ** 3
            task.release(:delay => delay)
            self.log_job_end(job.name, "#{retry_status}, retrying in #{delay}s") if job_started_at
          else # retries failed, bury
            task.bury
            self.log_job_end(job.name, "#{retry_status}, burying") if job_started_at
          end
        end
      end

      def work_one_task(task)
        job = Backburner::Job.new(task)
        self.log_job_begin(job.name, job.args)
        job.process(false)
        self.log_job_end(job.name)
        Process.exit
      rescue Backburner::Job::JobFormatInvalid => e
        self.log_error self.exception_message(e)
        Process.exit(false)
      rescue => e # Error occurred processing job
        self.log_error self.exception_message(e)
        handle_error(e, job.name, job.args)
        Process.exit(false)
      end

    end
  end
end
