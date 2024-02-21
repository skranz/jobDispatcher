example = function() {
  library(jobDispatcher)
  job_dir = "~/repbox/jobDispatcher/jobdir"
  input_df = data.frame(x=1:5)
  f = function(input) {
    5
  }
  jd = init_job_dispatcher(input_df, job_dir, job_fun = f, master_mode = "local")
  run_jobs(jd)

  jd = init_job_dispatcher(inputs, job_dir, job_fun = f, master_mode = "rstudio", worker_mode="rscript")
  run_jobs(jd)

  cat(jd$job_code)


  rstudioapi::filesPaneNavigate(job_dir)
}


init_job_dispatcher = function(inputs, job_dir, job_fun, num_worker=2, sleep=1, master_mode = c("local","rstudio")[2], worker_mode = c("rstudio")[1],  add_code = NULL) {
  restore.point("init_job_dispatcher")

  if (!dir.exists(job_dir)) stop(paste0("The job_dir ", job_dir, " does not exist."))
  if (worker_mode=="local") stop("worker_mode == local does not work.")

  job_dir = normalizePath(job_dir)

  fun_code = paste0(deparse(job_fun), collapse="\n")

  num_worker = min(num_worker, NROW(inputs))

  jd = list(
    master_mode = master_mode,
    worker_moder = worker_mode,
    job_dir = job_dir,
    input_dir = file.path(job_dir, "input"),
    output_dir = file.path(job_dir, "output"),
    status_dir = file.path(job_dir, "status"),
    script_dir = file.path(job_dir, "script"),
    inputs = inputs,
    num_worker = num_worker,
    num_jobs = NROW(inputs),
    fun_code = fun_code,
    sleep = sleep,
    running_jobs = rep(NA_integer_, num_worker),
    next_job = 1
  )
  jd
}

run_jobs = function(jd, clear=TRUE) {
  restore.point("run_jobs")

  cat("\nStart dispatcher job...")
  saveRDS(jd, file.path(jd$job_dir, "jd.Rds"))
  code = paste0('
setwd("', jd$job_dir,'")
library(jobDispatcher)
jd = readRDS("jd.Rds")
#restore.point.options(display.restore.point = TRUE)
jd_job_loop(jd, clear=', as.character(clear),')
    ')
  dispatch_file = file.path(jd$job_dir,"dispatch.R")
  writeLines(code,dispatch_file)
  jd_run_script(dispatch_file, jd$master_mode)


}

jd_job_loop = function(jd, clear=TRUE, show_memory = TRUE) {
  restore.point("jd_job_loop")
  if (clear) jd_clear(jd)
  jd$worker_runs = rep(FALSE, jd$num_worker)
  workers = 1:jd$num_worker
  for (worker_num in workers) {
    jd = jd_set_next_job_for_worker(worker_num, jd)
  }

  finished_jobs = 0
  Sys.sleep(1)
  new_line = TRUE
  cat("\n Start new jobs...")
  do_sleep = TRUE
  # We wait at least until one job has finished
  while(finished_jobs == 0 | jd$next_job <= jd$num_jobs) {
    if (do_sleep) {
      Sys.sleep(jd$sleep)
      if (show_memory) show_free_memory(new_line)
    }
    do_sleep = TRUE
    new_line = FALSE
    res = jd_find_next_finished_job(jd)
    if (!is.null(res)) {
      new_line=TRUE
      do_sleep = FALSE
      finished_jobs = finished_jobs+1
      if (res$type == "error") {
        cat(paste0("\nJob ", res$job_num, " had an error.\n"))
      } else {
        cat(paste0("\nJob ", res$job_num, " done by worker ", res$worker_num,".\n"))
      }
      jd = jd_set_next_job_for_worker(res$worker, jd)
    }
  }
  # Set finish signal for all jobs
  for (worker_num in workers) {
    finish_file = paste0(jd$input_dir,"/finish_", worker_num, ".Rds")
    saveRDS(0,finish_file)
  }

  cat("\nAll jobs have been dispatched. It might take some while until all workers are done, though.\n")
  invisible(jd)

}

jd_find_next_finished_job = function(jd) {
  # Check for finished jobs
  status_files = c(paste0(jd$status_dir,"/status_error__", jd$running_jobs))
  error = jd$running_jobs[file.exists(status_files)]

  status_files = c(paste0(jd$status_dir,"/status_done__", jd$running_jobs))
  done = jd$running_jobs[file.exists(status_files)]

  job_num = c(error, done)
  if (length(job_num)==0) return(NULL)

  job_num = job_num[1]
  type = if(length(error)==1) "error" else "done"
  worker_num = which(jd$running_jobs==job_num)

  list(job_num=job_num, worker_num=worker_num, type=type)

}

jd_set_next_job_for_worker = function(worker_num=NULL, jd) {
  restore.point("jd_set_next_job_for_worker")
  job_num = jd$next_job
  if (job_num > jd$num_jobs) return(jd)

  if (is.null(worker_num)) {
    worker_num = which(is.na(jd$running_jobs))[1]
  }
  cat("\nSet job ", job_num, " for worker ", worker_num,"\n.")

  if (is.data.frame(jd$inputs)) {
    input = jd$inputs[job_num,]
  } else {
    input = jd$inputs[[job_num]]
  }

  input_file = paste0(jd$input_dir,"/input_", worker_num, ".Rds")
  saveRDS(list(job_num=job_num, input=input),input_file)

  if (!jd$worker_runs[worker_num]) {
    jd$worker_runs[worker_num] = TRUE
    cat("\nStart job ", job_num," by worker ", worker_num, "\n")
    script_file = jd_make_worker_script(worker_num, jd)
    jd_run_script(script_file, jd$worker_mode)
  }

  jd$running_jobs[worker_num] = job_num
  jd$next_job = jd$next_job + 1
  jd
}

jd_run_script = function(script_file, mode) {
  if (mode=="local") {
    source(script_file, local=TRUE)
  } else if (mode=="rstudio") {
    rstudioapi::jobRunScript(script_file)
  # } else if (mode=="rscript") {
  #   cmd = paste0('Rscript "', script_file,'"')
  #   suppressMessages(system(cmd, wait=FALSE))
  # } else if (mode=="callr") {
  #   library(callr)
  #   callr::rscript(script_file, show=FALSE)
  } else {
    stop(paste0("Unknown mode ", mode))
  }
}



jd_make_worker_script = function(worker_num, jd) {
  code = paste0('
suppressMessages(library(jobDispatcher))
options(dplyr.summarise.inform = FALSE)
restorepoint::disable.restore.points()
setwd("', jd$job_dir,'")
job_dir = "',jd$job_dir,'"
worker_num = ', worker_num, '

my_job_fun = ', jd$fun_code,'

while(TRUE) {
  res = worker_wait_for_input(worker_num, job_dir)
  job_num = res$job_num
  input = res$input
  if (is.na(job_num)) {
#    saveRDS(res, file.path(job_dir, "test_', worker_num,'.Rds"))
    cat("\n\nThis worker has done all its jobs.\n\n")
    break
  }
  cat("
***************************************
  Worker ", worker_num, " starts job ", job_num, "...
****************************************
")
  result = try({
    my_job_fun(input)
  })

  if (is(result,"try-error")) {
    job_write_status(job_num, "error", job_dir)
  } else {
    job_write_status(job_num, "done", job_dir)
  }
}
')
  file = paste0(jd$script_dir, "/worker_",worker_num,".R")
  writeLines(code, file)
  file
}

job_write_status = function(job_num, status, job_dir = getwd()) {
  file = paste0(job_dir,"/status/status_",status,"__",job_num)
  writeLines("", file)
  cat("\n  Status: ", status,"\n")
}

jd_clear = function(jd) {
  restore.point("jd_clear")
  #stop()
  dir.create(jd$input_dir, FALSE)
  dir.create(jd$output_dir, FALSE)
  dir.create(jd$status_dir, FALSE)
  dir.create(jd$script_dir, FALSE)

  finish_files = list.files(jd$input_dir, glob2rx("finish_*.Rds"), full.names = TRUE)
  file.remove(finish_files)


  input_files = list.files(jd$input_dir, glob2rx("input_*.Rds"), full.names = TRUE)
  file.remove(input_files)

  output_files = list.files(jd$output_dir, glob2rx("output_*.Rds") , full.names = TRUE)
  file.remove(output_files)

  status_files = list.files(jd$status_dir, glob2rx("status_*"), full.names = TRUE)
  file.remove(status_files)

  script_files = list.files(jd$script_dir, glob2rx("worker_*"), full.names = TRUE)
  file.remove(script_files)



}

jd_rstudio_job = function(job_num, jd) {
  input=jd$inputs[[job_num]]
  input_file = file.path(jd$input_dir,"input_", job_num, ".Rds")
  saveRDS(input,input_file)

}

worker_wait_for_input = function(worker_num, job_dir, timeout = 10) {
  restore.point("worker_wait_for_input")

  finish_file = paste0(job_dir,"/input/finish_", worker_num, ".Rds")
  input_file = paste0(job_dir,"/input/input_", worker_num, ".Rds")
  sleep = 0.1
  start = as.numeric(Sys.time())
  while(TRUE) {
    if (as.numeric(Sys.time())-start > timeout) {
      cat(paste0("\n\nTimeout after waiting for ", timeout, " seconds for next job."))
      return(list(job_num=NA, input=NULL))
    }
    finish_exists = file.exists(finish_file)
    input_exists = file.exists(input_file)
    if (input_exists) break
    if (finish_exists) return(list(job_num=NA))
    Sys.sleep(sleep)
    sleep = min(sleep*2,2)
  }
  res = readRDS(input_file)
  cat(paste0("\nInput file found with job number ", res$job_num,"\n"))

  if (FALSE) {
    debug_dir = file.path(job_dir, "debug")
    dir.create(debug_dir, FALSE)
    test_file = paste0(debug_dir, "/input_job_", res$job_num,"_worker_",worker_num,".Rds")
    file.copy(input_file, test_file,overwrite = TRUE)
  }

  file.remove(input_file)
  res
}


job_load_input = function(worker_num, job_dir=getwd()) {
  input_dir = file.path(job_dir, "input")
  if (!dir.exists(input_dir))  stop(paste0("The job_dir ", job_dir, " has no input directory."))
  input_file = paste0(input_dir,"/input_", worker_num, ".Rds")
  readRDS(input_file)
}


job_save_output = function(x,job_num, job_dir=getwd()) {
  dir = file.path(job_dir, "output")
  if (!dir.exists(dir))  stop(paste0("The job_dir ", job_dir, " has no output directory."))
  file = paste0(dir,"/output_", job_num, ".Rds")
  saveRDS(x,file)
}


