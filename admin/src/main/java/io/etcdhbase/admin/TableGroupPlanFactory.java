package io.etcdhbase.admin;

import io.etcdhbase.server.config.RuntimeConfiguration;
import io.etcdhbase.server.config.RuntimeConfiguration.HistoryLayout;
import io.etcdhbase.server.config.RuntimeConfiguration.TableGroup;
import java.util.List;

/** Resolves one table group and one explicitly configured or requested history layout. */
final class TableGroupPlanFactory {
  private TableGroupPlanFactory() {}

  static TableGroupInitializationPlan create(
      RuntimeConfiguration configuration,
      String requestedTableGroupId,
      HistoryLayout requestedHistoryLayout) {
    TableGroup tableGroup = selectTableGroup(configuration.tableGroups(), requestedTableGroupId);
    HistoryLayout configuredLayout = tableGroup.historyLayout();
    HistoryLayout selectedLayout;
    if (configuredLayout == HistoryLayout.UNDECIDED) {
      if (requestedHistoryLayout == null || requestedHistoryLayout == HistoryLayout.UNDECIDED) {
        throw new IllegalArgumentException(
            "history layout is undecided; provide --history-layout explicitly");
      }
      selectedLayout = requestedHistoryLayout;
    } else {
      if (requestedHistoryLayout != null && requestedHistoryLayout != configuredLayout) {
        throw new IllegalArgumentException(
            "requested history layout conflicts with the selected table group configuration");
      }
      selectedLayout = configuredLayout;
    }
    return TableGroupInitializationPlan.create(configuration, tableGroup, selectedLayout);
  }

  private static TableGroup selectTableGroup(List<TableGroup> tableGroups, String requestedId) {
    if (requestedId == null) {
      if (tableGroups.size() != 1) {
        throw new IllegalArgumentException(
            "provide --table-group when the configuration has multiple groups");
      }
      return tableGroups.getFirst();
    }
    return tableGroups.stream()
        .filter(tableGroup -> tableGroup.id().equals(requestedId))
        .findFirst()
        .orElseThrow(
            () -> new IllegalArgumentException("requested table group is not configured"));
  }
}
