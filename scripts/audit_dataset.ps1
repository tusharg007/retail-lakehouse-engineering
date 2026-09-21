param(
    [string]$DataDirectory = (Join-Path $PSScriptRoot '../walmart_dataset/data'),
    [string]$OutputPath = (Join-Path $PSScriptRoot '../docs/evidence/dataset-audit.json')
)
$ErrorActionPreference = 'Stop'
$culture = [System.Globalization.CultureInfo]::InvariantCulture
function Money([string]$value) { return [decimal]::Parse($value, $culture) }
$keys = [ordered]@{
    customers = 'customer_id'; employees = 'employee_id'; order_items = 'order_item_id'
    orders = 'order_id'; products = 'product_id'; stores = 'store_id'
}
$tables = @{}
$indexes = @{}
$summaries = @()
foreach ($name in $keys.Keys) {
    $path = Join-Path $DataDirectory "$name.csv"
    $rows = @(Import-Csv -LiteralPath $path)
    if ($rows.Count -eq 0) { throw "Empty input: $name" }
    $key = $keys[$name]
    if ($key -notin $rows[0].PSObject.Properties.Name) { throw "Missing key: $name.$key" }
    $index = @{}
    $duplicateRows = 0
    $blankKeys = 0
    foreach ($row in $rows) {
        $id = $row.$key
        if ([string]::IsNullOrWhiteSpace($id)) { $blankKeys++; continue }
        if ($index.ContainsKey($id)) { $duplicateRows++ }
        $index[$id] = $row
    }
    $tables[$name] = $rows
    $indexes[$name] = $index
    $summaries += [ordered]@{
        table = $name; rows = $rows.Count; primary_key = $key
        duplicate_key_rows = $duplicateRows; blank_keys = $blankKeys
        columns = @($rows[0].PSObject.Properties.Name)
        sha256 = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
        minimum_updated_timestamp = ($rows.updated_timestamp | Sort-Object | Select-Object -First 1)
        maximum_updated_timestamp = ($rows.updated_timestamp | Sort-Object | Select-Object -Last 1)
        is_active_values = @($rows.is_active | Sort-Object -Unique)
    }
}
$relations = @(
    @('employees', 'store_id', 'stores'), @('orders', 'customer_id', 'customers'),
    @('orders', 'store_id', 'stores'), @('order_items', 'order_id', 'orders'),
    @('order_items', 'product_id', 'products')
)
$relationshipChecks = @()
foreach ($relation in $relations) {
    $missing = 0
    foreach ($row in $tables[$relation[0]]) {
        $value = $row.($relation[1])
        if ([string]::IsNullOrWhiteSpace($value) -or !$indexes[$relation[2]].ContainsKey($value)) { $missing++ }
    }
    $relationshipChecks += [ordered]@{
        child = $relation[0]; column = $relation[1]; parent = $relation[2]; missing_parent_rows = $missing
    }
}
$staffCounts = @{}
foreach ($employee in $tables.employees) { $staffCounts[$employee.store_id]++ }
$lineSums = @{}
$itemCounts = @{}
[decimal]$lineTotal = 0
[decimal]$orderTotal = 0
[decimal]$fanoutTotal = 0
[long]$fanoutRows = 0
$lineMismatches = 0
$orderMismatches = 0
foreach ($item in $tables.order_items) {
    $amount = Money $item.line_amount
    $lineTotal += $amount
    $lineSums[$item.order_id] += $amount
    $itemCounts[$item.order_id]++
    if ((Money $item.quantity) * (Money $item.unit_price) -ne $amount) { $lineMismatches++ }
    $order = $indexes.orders[$item.order_id]
    $staff = 1
    if ($null -ne $order -and $staffCounts.ContainsKey($order.store_id)) { $staff = $staffCounts[$order.store_id] }
    $fanoutRows += $staff
    $fanoutTotal += $amount * $staff
}
foreach ($order in $tables.orders) {
    $amount = Money $order.total_amount
    $orderTotal += $amount
    if ($amount -ne $lineSums[$order.order_id]) { $orderMismatches++ }
}
$report = [ordered]@{
    audit_scope = 'Supplied CSV files; static analysis, not a pipeline execution.'
    tables = $summaries
    relationships = $relationshipChecks
    reconciliation = [ordered]@{
        order_line_rows = $tables.order_items.Count; line_amount_sum = $lineTotal
        order_total_amount_sum = $orderTotal; line_arithmetic_mismatches = $lineMismatches
        order_total_mismatches = $orderMismatches
        orders_with_multiple_lines = @($itemCounts.Values | Where-Object { $_ -gt 1 }).Count
        employee_store_join_rows = $fanoutRows; employee_store_join_line_amount_sum = $fanoutTotal
        currency = 'Unspecified in source; sums include all statuses and active flags.'
    }
    observed_values = [ordered]@{
        order_status = @($tables.orders.order_status | Sort-Object -Unique)
        payment_method = @($tables.orders.payment_method | Sort-Object -Unique)
        customer_email_domains = @($tables.customers.email | ForEach-Object { ($_ -split '@')[-1] } | Sort-Object -Unique)
    }
}
$parent = Split-Path -Parent $OutputPath
if ($parent -and !(Test-Path -LiteralPath $parent)) { New-Item -ItemType Directory -Path $parent -Force | Out-Null }
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $OutputPath -Encoding UTF8
Write-Output "Dataset audit written to $OutputPath"
