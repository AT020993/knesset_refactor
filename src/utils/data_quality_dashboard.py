"""
Data quality monitoring dashboard for the Knesset data processing system.

This module provides a comprehensive dashboard to monitor data quality metrics,
run validation tests, and display results in an easy-to-understand format.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import streamlit as st

from backend.connection_manager import get_db_connection

from .aggregation_validator import AggregationValidator
from .data_validator import DataValidator
from .date_validator import DateValidator
from .faction_resolver import FactionResolver


class DataQualityDashboard:
    """Comprehensive data quality monitoring dashboard."""

    def __init__(self, db_path: Path, logger_obj: logging.Logger):
        self.db_path = db_path
        self.logger = logger_obj
        self.data_validator = DataValidator(db_path, logger_obj)
        self.aggregation_validator = AggregationValidator(db_path, logger_obj)
        self.date_validator = DateValidator(db_path, logger_obj)

    def create_dashboard(self) -> None:
        """Create the main data quality dashboard."""
        st.title("🔍 Data Quality Monitoring Dashboard")
        st.markdown("Comprehensive monitoring and validation of Knesset data processing system")

        # Create tabs for different validation areas
        tab1, tab2, tab3, tab4, tab5 = st.tabs(
            [
                "📊 Overview",
                "🏛️ Data Integrity",
                "📈 Aggregation Validation",
                "📅 Date Consistency",
                "🔧 Diagnostic Tools",
            ]
        )

        with tab1:
            self._create_overview_tab()

        with tab2:
            self._create_data_integrity_tab()

        with tab3:
            self._create_aggregation_validation_tab()

        with tab4:
            self._create_date_consistency_tab()

        with tab5:
            self._create_diagnostic_tools_tab()

    def _create_overview_tab(self) -> None:
        """Create the overview tab with high-level metrics."""
        st.header("Data Quality Overview")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Database Status", "✅ Connected" if self.db_path.exists() else "❌ Not Found", delta=None)

        with col2:
            # Get basic database stats
            try:
                with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                    table_count = con.execute(
                        "SELECT COUNT(*) as count FROM duckdb_tables() WHERE schema_name='main'"
                    ).fetchone()[0]

                st.metric("Tables Available", table_count)
            except Exception as e:
                st.metric("Tables Available", "Error", delta=str(e))

        with col3:
            st.metric("Last Update", datetime.now().strftime("%Y-%m-%d %H:%M"), delta=None)

        # Quick validation summary
        if st.button("Run Quick Validation Check"):
            with st.spinner("Running validation checks..."):
                results = self._run_quick_validation()
                self._display_validation_summary(results)

    def _create_data_integrity_tab(self) -> None:
        """Create the data integrity validation tab."""
        st.header("Data Integrity Validation")

        col1, col2 = st.columns(2)

        with col1:
            if st.button("🔍 Validate Bill Initiator Counting"):
                with st.spinner("Validating bill initiator logic..."):
                    results = self.data_validator.validate_bill_initiator_counts()
                    self._display_bill_initiator_results(results)

        with col2:
            if st.button("🏛️ Validate Committee Join Logic"):
                with st.spinner("Validating committee joins..."):
                    results = self.data_validator.validate_committee_join_consistency()
                    self._display_committee_join_results(results)

        # Faction resolution validation
        st.subheader("Faction Resolution Validation")
        if st.button("👥 Validate Faction Resolution"):
            with st.spinner("Validating faction resolution consistency..."):
                validation_queries = FactionResolver.validate_faction_resolution_consistency()
                self._run_and_display_validation_queries(validation_queries, "Faction Resolution")

    def _create_aggregation_validation_tab(self) -> None:
        """Create the aggregation validation tab."""
        st.header("Aggregation Logic Validation")

        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("🌐 Network Charts"):
                with st.spinner("Validating network chart aggregations..."):
                    results = self.aggregation_validator.validate_network_chart_aggregations()
                    self._display_aggregation_results(results, "Network Charts")

        with col2:
            if st.button("📊 Comparison Charts"):
                with st.spinner("Validating comparison chart aggregations..."):
                    results = self.aggregation_validator.validate_comparison_chart_aggregations()
                    self._display_aggregation_results(results, "Comparison Charts")

        with col3:
            if st.button("📈 Time Series"):
                with st.spinner("Validating time series aggregations..."):
                    results = self.aggregation_validator.validate_time_series_aggregations()
                    self._display_aggregation_results(results, "Time Series")

        # Run comprehensive aggregation validation
        if st.button("🔄 Run All Aggregation Validations"):
            with st.spinner("Running comprehensive aggregation validation..."):
                results = self.aggregation_validator.run_comprehensive_aggregation_validation()
                self._display_comprehensive_results(results, "Aggregation Validation")

    def _create_date_consistency_tab(self) -> None:
        """Create the date consistency validation tab."""
        st.header("Date Consistency Validation")

        col1, col2 = st.columns(2)

        with col1:
            if st.button("📅 Bill Submission Dates"):
                with st.spinner("Validating bill submission dates..."):
                    results = self.date_validator.validate_bill_submission_dates()
                    self._display_date_results(results, "Bill Submission Dates")

        with col2:
            if st.button("🏛️ Knesset Date Alignment"):
                with st.spinner("Validating Knesset date consistency..."):
                    results = self.date_validator.validate_knesset_date_consistency()
                    self._display_date_results(results, "Knesset Date Alignment")

        # Session date validation
        if st.button("🗓️ Session Date Validation"):
            with st.spinner("Validating session date consistency..."):
                results = self.date_validator.validate_session_date_consistency()
                self._display_date_results(results, "Session Dates")

        # Run comprehensive date validation
        if st.button("🔄 Run All Date Validations"):
            with st.spinner("Running comprehensive date validation..."):
                results = self.date_validator.run_comprehensive_date_validation()
                self._display_comprehensive_results(results, "Date Validation")

    def _create_diagnostic_tools_tab(self) -> None:
        """Create the diagnostic tools tab."""
        st.header("Diagnostic Tools")

        # Data correction suggestions
        st.subheader("Data Correction Suggestions")

        col1, col2 = st.columns(2)

        with col1:
            if st.button("🔧 Committee Resolution Issues"):
                try:
                    from .committee_resolver import CommitteeResolver

                    fixes = CommitteeResolver.get_committee_data_fixes()
                    self._run_and_display_diagnostic_queries(fixes, "Committee Issues")
                except Exception as e:
                    st.error(f"Error running committee diagnostics: {e}")

        with col2:
            if st.button("📅 Date Correction Suggestions"):
                fixes = self.date_validator.get_date_correction_suggestions()
                self._run_and_display_diagnostic_queries(fixes, "Date Issues")

        # Manual query runner
        st.subheader("Manual Query Runner")
        query_input = st.text_area("Enter SQL Query", height=150, placeholder="SELECT * FROM KNS_Bill LIMIT 10;")

        if st.button("Execute Query"):
            if query_input.strip():
                self._execute_manual_query(query_input)
            else:
                st.warning("Please enter a query")

    def _run_quick_validation(self) -> Dict[str, Any]:
        """Run a quick validation check."""
        results = {"timestamp": datetime.now().isoformat(), "checks": []}

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                # Quick checks
                checks = [
                    ("Tables Available", "SELECT COUNT(*) FROM duckdb_tables() WHERE schema_name='main'"),
                    ("Total Bills", "SELECT COUNT(*) FROM KNS_Bill"),
                    ("Total Queries", "SELECT COUNT(*) FROM KNS_Query"),
                    ("Total MKs", "SELECT COUNT(*) FROM KNS_Person"),
                    ("Total Factions", "SELECT COUNT(*) FROM KNS_Faction"),
                ]

                for check_name, query in checks:
                    try:
                        result = con.execute(query).fetchone()[0]
                        results["checks"].append({"name": check_name, "value": result, "status": "success"})
                    except Exception as e:
                        results["checks"].append({"name": check_name, "value": 0, "status": "error", "error": str(e)})

        except Exception as e:
            results["error"] = str(e)

        return results

    def _display_validation_summary(self, results: Dict[str, Any]) -> None:
        """Display validation summary results."""
        if "error" in results:
            st.error(f"Validation failed: {results['error']}")
            return

        st.subheader("Quick Validation Results")

        cols = st.columns(len(results["checks"]))

        for i, check in enumerate(results["checks"]):
            with cols[i]:
                if check["status"] == "success":
                    st.metric(check["name"], f"{check['value']:,}")
                else:
                    st.metric(check["name"], "Error", delta=check.get("error", "Unknown"))

    def _display_bill_initiator_results(self, results: Dict[str, Any]) -> None:
        """Display bill initiator validation results."""
        if results.get("validation_status") == "error":
            st.error(f"Validation failed: {results.get('error_message', 'Unknown error')}")
            return

        st.subheader("Bill Initiator Validation Results")

        if results.get("validation_passed"):
            st.success("✅ All bill initiator counting validation tests passed!")
        else:
            st.warning("⚠️ Bill initiator counting issues detected")

        # Display metrics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Bills without Primary Initiators", results.get("bills_without_primary_initiators", 0))

        with col2:
            st.metric("Bills with Multiple Primaries", results.get("bills_with_multiple_primaries", 0))

        with col3:
            st.metric("Counting Inconsistencies", results.get("bills_with_counting_inconsistencies", 0))

    def _display_committee_join_results(self, results: Dict[str, Any]) -> None:
        """Display committee join validation results."""
        if results.get("validation_status") == "error":
            st.error(f"Validation failed: {results.get('error_message', 'Unknown error')}")
            return

        st.subheader("Committee Join Validation Results")

        if results.get("validation_passed"):
            st.success("✅ Committee join validation passed!")
        else:
            st.warning("⚠️ Committee join issues detected")

        # Display join statistics
        join_stats = results.get("committee_join_stats", {})
        if join_stats:
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Bills with Committee ID", join_stats.get("bills_with_committee_id", 0))

            with col2:
                st.metric("Successful Joins", join_stats.get("successful_joins", 0))

            with col3:
                st.metric("Join Success Rate", f"{join_stats.get('join_success_rate', 0):.1f}%")

    def _display_aggregation_results(self, results: Dict[str, Any], chart_type: str) -> None:
        """Display aggregation validation results."""
        if results.get("validation_status") == "error":
            st.error(f"Validation failed: {results.get('error_message', 'Unknown error')}")
            return

        st.subheader(f"{chart_type} Validation Results")

        if results.get("validation_passed"):
            st.success(f"✅ {chart_type} aggregation validation passed!")
        else:
            st.warning(f"⚠️ {chart_type} aggregation issues detected")

        # Display specific metrics based on chart type
        for key, value in results.items():
            if isinstance(value, dict) and "total" in str(key).lower():
                st.json(value)

    def _display_date_results(self, results: Dict[str, Any], date_type: str) -> None:
        """Display date validation results."""
        if results.get("validation_status") == "error":
            st.error(f"Validation failed: {results.get('error_message', 'Unknown error')}")
            return

        st.subheader(f"{date_type} Validation Results")

        if results.get("validation_passed"):
            st.success(f"✅ {date_type} validation passed!")
        else:
            st.warning(f"⚠️ {date_type} issues detected")

        # Display date-specific metrics
        for key, value in results.items():
            if isinstance(value, dict) and any(
                word in key.lower() for word in ["date", "chronological", "consistency"]
            ):
                with st.expander(f"📊 {key.replace('_', ' ').title()}"):
                    st.json(value)

    def _display_comprehensive_results(self, results: Dict[str, Any], validation_type: str) -> None:
        """Display comprehensive validation results."""
        st.subheader(f"Comprehensive {validation_type} Results")

        if results.get("overall_validation_passed"):
            st.success(f"✅ All {validation_type.lower()} tests passed!")
        else:
            st.error(f"❌ {validation_type} issues detected")

        # Summary metrics
        summary = results.get("summary", {})
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Tests Run", summary.get("tests_run", 0))

        with col2:
            st.metric("Tests Passed", summary.get("tests_passed", 0))

        with col3:
            pass_rate = (summary.get("tests_passed", 0) / summary.get("tests_run", 1)) * 100
            st.metric("Pass Rate", f"{pass_rate:.1f}%")

        # Issues list
        issues = (
            summary.get("critical_issues", [])
            or summary.get("aggregation_issues", [])
            or summary.get("date_issues", [])
        )
        if issues:
            st.subheader("Issues Detected")
            for issue in issues:
                st.warning(f"⚠️ {issue}")

    def _run_and_display_validation_queries(self, queries: Dict[str, str], section_name: str) -> None:
        """Run and display validation queries."""
        st.subheader(f"{section_name} Validation")

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                for query_name, query_sql in queries.items():
                    with st.expander(f"📊 {query_name.replace('_', ' ').title()}"):
                        try:
                            df = con.execute(query_sql).df()
                            if not df.empty:
                                st.dataframe(df, use_container_width=True)

                                # Add download button
                                csv = df.to_csv(index=False)
                                st.download_button(
                                    label=f"Download {query_name} results",
                                    data=csv,
                                    file_name=f"{query_name}_results.csv",
                                    mime="text/csv",
                                )
                            else:
                                st.info("No issues found - this is good!")
                        except Exception as e:
                            st.error(f"Error running query: {e}")
        except Exception as e:
            st.error(f"Database connection error: {e}")

    def _run_and_display_diagnostic_queries(self, queries: Dict[str, str], section_name: str) -> None:
        """Run and display diagnostic queries."""
        st.subheader(f"{section_name} Diagnostics")

        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                for query_name, query_sql in queries.items():
                    with st.expander(f"🔧 {query_name.replace('_', ' ').title()}"):
                        try:
                            df = con.execute(query_sql).df()
                            st.dataframe(df, use_container_width=True)

                            if not df.empty:
                                # Add download button
                                csv = df.to_csv(index=False)
                                st.download_button(
                                    label=f"Download {query_name} diagnostics",
                                    data=csv,
                                    file_name=f"{query_name}_diagnostics.csv",
                                    mime="text/csv",
                                )
                        except Exception as e:
                            st.error(f"Error running diagnostic query: {e}")
        except Exception as e:
            st.error(f"Database connection error: {e}")

    def _execute_manual_query(self, query: str) -> None:
        """Execute a manual query and display results."""
        try:
            with get_db_connection(self.db_path, read_only=True, logger_obj=self.logger) as con:
                df = con.execute(query).df()

                if not df.empty:
                    st.success(f"Query executed successfully. Returned {len(df)} rows.")
                    st.dataframe(df, use_container_width=True)

                    # Add download button
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="Download query results", data=csv, file_name="manual_query_results.csv", mime="text/csv"
                    )
                else:
                    st.info("Query executed successfully but returned no results.")

        except Exception as e:
            st.error(f"Query execution error: {e}")


def create_data_quality_monitoring_page(db_path: Path, logger_obj: logging.Logger) -> None:
    """Create a complete data quality monitoring page."""
    dashboard = DataQualityDashboard(db_path, logger_obj)
    dashboard.create_dashboard()
