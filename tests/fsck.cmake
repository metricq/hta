find_package(Python3 COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND)
    add_executable(hta_fsck_fixture ${CMAKE_CURRENT_LIST_DIR}/fsck_fixture.cpp)
    target_link_libraries(hta_fsck_fixture PRIVATE hta::hta)
    add_library(hta_fsck_fault SHARED ${CMAKE_CURRENT_LIST_DIR}/fsck_fault.cpp)
    target_link_libraries(hta_fsck_fault PRIVATE ${CMAKE_DL_LIBS})
    add_test(NAME hta.fsck COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_LIST_DIR}/fsck_integration.py
        $<TARGET_FILE:hta_fsck> $<TARGET_FILE:hta_repair> $<TARGET_FILE:hta_fsck_fixture>
        $<TARGET_FILE:hta_fsck_fault>)
    set_tests_properties(hta.fsck PROPERTIES TIMEOUT 180)
    find_program(HTA_VALGRIND valgrind)
    if(HTA_VALGRIND)
        add_test(NAME hta.fsck.valgrind COMMAND ${CMAKE_COMMAND} -E env
            "HTA_FSCK_VALGRIND=${HTA_VALGRIND}"
            "HTA_FSCK_VALGRIND_LOGS=${CMAKE_CURRENT_BINARY_DIR}/fsck-valgrind"
            ${Python3_EXECUTABLE} ${CMAKE_CURRENT_LIST_DIR}/fsck_integration.py
            $<TARGET_FILE:hta_fsck> $<TARGET_FILE:hta_repair> $<TARGET_FILE:hta_fsck_fixture>
            $<TARGET_FILE:hta_fsck_fault>
            Recovery.test_healthy_metric_is_unchanged
            Recovery.test_database_mode_preserves_healthy_metrics
            Recovery.test_database_mode_continues_after_failure
            Recovery.test_database_rollback
            Recovery.test_rebuild_first_interval
            Recovery.test_raw_tail_loss_removes_unclosed_aggregates_not_shortens_them
            Recovery.test_full_detects_isolated_old_corruption
            Recovery.test_lower_damage_propagates_past_matching_upper_tail
            Recovery.test_partial_upper_with_missing_lower
            Recovery.test_unchanged_files_and_prefix_preserved
            Recovery.test_invalid_raw_header_no_writes
            Recovery.test_rollback_restores_corrupted_input_exactly
            Recovery.test_zero_and_single_point_metrics
            Recovery.test_missing_empty_levels_and_short_healthy_metrics
            Recovery.test_interrupted_rollback_blocks_checks_until_resumed
            Recovery.test_rollback_state_write_failure_keeps_repaired_files
            Recovery.test_explicit_modes_reject_wrong_layout_without_writes
            Recovery.test_backup_directories_are_skipped_in_all_database_modes
            Recovery.test_backup_pattern_does_not_skip_other_metric_names
            Recovery.test_metric_flag_can_repair_and_rollback_a_backup_directory
            Recovery.test_no_journal_rollback_is_unchanged_in_both_modes
            Recovery.test_streaming_raw_read_budget_for_full_scan_and_rebuild
            Recovery.test_terminal_progress_completed_list_and_eta
            Recovery.test_terminal_failure_warning_and_empty_database
            Recovery.test_plain_logs_for_pipes_and_dumb_terminal
            Recovery.test_narrow_terminal_live_lines_do_not_wrap
            Recovery.test_completed_rollback_allows_checks_and_requires_archive_for_repair
            Recovery.test_symlink_paths_are_explicitly_refused_without_touching_targets
            Recovery.test_database_symlinks_report_errors_or_can_be_excluded
            Recovery.test_lost_found_is_an_explicit_exclusion_not_a_name_heuristic
            Recovery.test_terminal_dry_run_shows_same_per_level_plan_as_pipe
            Recovery.test_terminal_verbose_and_recovery_hints
            Recovery.test_control_characters_in_paths_are_escaped_in_logs_and_terminal
            Recovery.test_preflight_errors_escape_paths_without_progress_object
            Recovery.test_raw_header_size_version_and_period_are_validated
            Harness)
        set_tests_properties(hta.fsck.valgrind PROPERTIES TIMEOUT 420 LABELS memory)
    endif()
endif()
