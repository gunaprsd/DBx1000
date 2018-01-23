#include "global.h"
#include "helper.h"

void print_usage() {
  printf("[usage]:\n");
  printf("\t-pINT       ; PART_CNT\n");
  printf("\t-vINT       ; VIRTUAL_PART_CNT\n");
  printf("\t-tINT       ; THREAD_CNT\n");
  printf("\t-qINT       ; QUERY_INTVL\n");
  printf("\t-dINT       ; PRT_LAT_DISTR\n");
  printf("\t-aINT       ; PART_ALLOC (0 or 1)\n");
  printf("\t-mINT       ; MEM_PAD (0 or 1)\n");
  printf("\t-GaINT      ; ABORT_PENALTY (in ms)\n");
  printf("\t-GcINT      ; CENTRAL_MAN\n");
  printf("\t-GtINT      ; TS_ALLOC\n");
  printf("\t-GkINT      ; KEY_ORDER\n");
  printf("\t-GnINT      ; NO_DL\n");
  printf("\t-GoINT      ; TIMEOUT\n");
  printf("\t-GlINT      ; DL_LOOP_DETECT\n");

  printf("\t-GbINT      ; TS_BATCH_ALLOC\n");
  printf("\t-GuINT      ; TS_BATCH_NUM\n");

  printf("\t-o STRING   ; output file\n\n");
  printf("  [YCSB]:\n");
  printf("\t-cINT       ; PART_PER_TXN\n");
  printf("\t-eINT       ; PERC_MULTI_PART\n");
  printf("\t-rFLOAT     ; READ_PERC\n");
  printf("\t-wFLOAT     ; WRITE_PERC\n");
  printf("\t-zFLOAT     ; ZIPF_THETA\n");
  printf("\t-sINT       ; SYNTH_TABLE_SIZE\n");
  printf("\t-RINT       ; REQ_PER_QUERY\n");
  printf("\t-fINT       ; FIELD_PER_TUPLE\n");
  printf("  [TPCC]:\n");
  printf("\t-nINT       ; NUM_WH\n");
  printf("\t-TpFLOAT    ; PERC_PAYMENT\n");
  printf("\t-TuINT      ; WH_UPDATE\n");
  printf("  [TEST]:\n");
  printf("\t-Ar         ; Test READ_WRITE\n");
  printf("\t-Ac         ; Test CONFLICT\n");
  printf("  [EXPERIMENT]:\n");
  printf("\t-EcFLOAT	  ; CONTENTION_PERC\n");
  printf("\t-EpINT	  ; POS_IN_TXN\n");
  printf("\t-ElINT	  ; TXN_LENGTH\n");
}

void parser(int argc, char **argv) {
  g_params["abort_buffer_enable"] = ABORT_BUFFER_ENABLE ? "true" : "false";
  g_params["write_copy_form"] = WRITE_COPY_FORM;
  g_params["validation_lock"] = VALIDATION_LOCK;
  g_params["pre_abort"] = PRE_ABORT;
  g_params["atomic_timestamp"] = ATOMIC_TIMESTAMP;

  for (int i = 1; i < argc; i++) {
    assert(argv[i][0] == '-');

    if (argv[i][1] == 'd')
      g_data_folder = string(&argv[i][2]);
    else if (argv[i][1] == 't')
      g_thread_cnt = atoi(&argv[i][2]);
    else if (argv[i][1] == 's')
      g_size_factor = atoi(&argv[i][2]);
    else if (argv[i][1] == 'o') {
      i++;
      output_file = argv[i];
    } else if (argv[i][1] == 'h') {
      print_usage();
      exit(0);
    } else if (argv[i][1] == '-') {
      string line(&argv[i][2]);
      size_t pos = line.find("=");
      assert(pos != string::npos);
      string name = line.substr(0, pos);
      string value = line.substr(pos + 1, line.length());
      assert(g_params.find(name) != g_params.end());
      g_params[name] = value;
    } else if (argv[i][1] == 'P') {

      switch (argv[i][2]) {
      case 'b':
        g_benchmark = &argv[i][3];
        break;
      case 't':
        if (argv[i][3] == '2') {
          g_benchmark_tag2 = &argv[i][4];
        } else {
          g_benchmark_tag = &argv[i][3];
        }
        break;
      case 'u':
        g_ufactor = atoi(&argv[i][3]);
        break;
      case 'g':
        g_task_type = GENERATE;
        break;
      case 'p':
        switch (argv[i][3]) {
        case 'a':
          g_task_type = PARTITION_DATA;
          break;
        case 'c':
          g_task_type = PARTITION_CONFLICT;
          break;
        default:
          assert(false);
        }
        break;
      case 'e':
        switch (argv[i][3]) {
        case 'r':
          g_task_type = EXECUTE_RAW;
          break;
        case 'p':
          g_task_type = EXECUTE_PARTITIONED;
          break;
        default:
          assert(false);
        }
        break;
      case 'c':
	g_op_cost = atoi(&argv[i][3]);
	break;
      default:
        assert(false);
      }
    } else {
      assert(false);
    }
  }
  if (g_thread_cnt < g_init_parallelism)
    g_init_parallelism = g_thread_cnt;
}
