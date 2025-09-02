"""Network and connection chart generators."""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Tuple
import math

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
import numpy as np

from backend.connection_manager import get_db_connection, safe_execute_query
from utils.faction_resolver import FactionResolver, get_faction_name_field, get_coalition_status_field
from .base import BaseChart


class NetworkCharts(BaseChart):
    """Network analysis charts (connection maps, collaboration networks, etc.)."""

    def plot_mk_collaboration_network(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        min_collaborations: int = 3,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate MK collaboration network chart showing individual member connections."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b", **kwargs)

        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(con, ["KNS_Bill", "KNS_BillInitiator", "KNS_Person", "KNS_PersonToPosition", "KNS_Faction"]):
                    return None

                # Get MK collaboration network data using standardized faction resolution
                query = f"""
                WITH {FactionResolver.get_standard_faction_lookup_cte()},
                BillCollaborations AS (
                    SELECT 
                        main.PersonID as MainInitiatorID,
                        supp.PersonID as SupporterID,
                        COUNT(DISTINCT main.BillID) as CollaborationCount
                    FROM KNS_BillInitiator main
                    JOIN KNS_Bill b ON main.BillID = b.BillID
                    JOIN KNS_BillInitiator supp ON main.BillID = supp.BillID 
                    WHERE main.Ordinal = 1 
                        AND supp.Ordinal > 1
                        AND b.KnessetNum IS NOT NULL
                        AND {filters["knesset_condition"]}
                    GROUP BY main.PersonID, supp.PersonID
                    HAVING COUNT(DISTINCT main.BillID) >= {min_collaborations}
                ),
                MKDetails AS (
                    SELECT 
                        p.PersonID,
                        p.FirstName || ' ' || p.LastName as FullName,
                        {get_faction_name_field('f', "'Independent'")} as FactionName,
                        COUNT(DISTINCT bi.BillID) as TotalBills
                    FROM KNS_Person p
                    LEFT JOIN KNS_BillInitiator bi ON p.PersonID = bi.PersonID AND bi.Ordinal = 1
                    LEFT JOIN StandardFactionLookup sfl ON p.PersonID = sfl.PersonID AND sfl.rn = 1
                    LEFT JOIN KNS_Faction f ON sfl.FactionID = f.FactionID
                    WHERE p.PersonID IN (
                        SELECT MainInitiatorID FROM BillCollaborations
                        UNION
                        SELECT SupporterID FROM BillCollaborations
                    )
                    GROUP BY p.PersonID, p.FirstName, p.LastName, f.Name
                )
                SELECT 
                    bc.MainInitiatorID,
                    bc.SupporterID,
                    bc.CollaborationCount,
                    main_mk.FullName as MainInitiatorName,
                    main_mk.FactionName as MainInitiatorFaction,
                    main_mk.TotalBills as MainInitiatorTotalBills,
                    supp_mk.FullName as SupporterName,
                    supp_mk.FactionName as SupporterFaction,
                    supp_mk.TotalBills as SupporterTotalBills
                FROM BillCollaborations bc
                JOIN MKDetails main_mk ON bc.MainInitiatorID = main_mk.PersonID
                JOIN MKDetails supp_mk ON bc.SupporterID = supp_mk.PersonID
                ORDER BY bc.CollaborationCount DESC
                """

                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(f"No MK collaboration data found for '{filters['knesset_title']}' with minimum {min_collaborations} collaborations.")
                    return None

                # Create network visualization
                return self._create_mk_network_chart(df, filters['knesset_title'])

        except Exception as e:
            self.logger.error(f"Error generating MK collaboration network: {e}", exc_info=True)
            st.error(f"Could not generate MK collaboration network: {e}")
            return None

    def plot_faction_collaboration_network(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        min_collaborations: int = 5,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate faction collaboration network chart showing inter-faction connections."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b", **kwargs)

        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(con, ["KNS_Bill", "KNS_BillInitiator", "KNS_PersonToPosition", "KNS_Faction"]):
                    return None

                # Get faction collaboration network data with proper faction bill counting
                query = f"""
                WITH FactionCollaborations AS (
                    SELECT 
                        main.PersonID as MainPersonID,
                        supp.PersonID as SuppPersonID,
                        main.BillID,
                        b.KnessetNum
                    FROM KNS_BillInitiator main
                    JOIN KNS_Bill b ON main.BillID = b.BillID
                    JOIN KNS_BillInitiator supp ON main.BillID = supp.BillID 
                    WHERE main.Ordinal = 1 
                        AND supp.Ordinal > 1
                        AND b.KnessetNum IS NOT NULL
                        AND {filters["knesset_condition"]}
                ),
                PersonFactions AS (
                    SELECT DISTINCT
                        fc.MainPersonID as PersonID,
                        COALESCE(
                            (SELECT f.FactionID 
                             FROM KNS_PersonToPosition ptp 
                             JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                             WHERE ptp.PersonID = fc.MainPersonID AND ptp.KnessetNum = fc.KnessetNum
                             ORDER BY ptp.StartDate DESC LIMIT 1),
                            (SELECT f.FactionID 
                             FROM KNS_PersonToPosition ptp 
                             JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                             WHERE ptp.PersonID = fc.MainPersonID
                             ORDER BY ptp.KnessetNum DESC, ptp.StartDate DESC LIMIT 1)
                        ) as FactionID
                    FROM FactionCollaborations fc
                    UNION
                    SELECT DISTINCT
                        fc.SuppPersonID as PersonID,
                        COALESCE(
                            (SELECT f.FactionID 
                             FROM KNS_PersonToPosition ptp 
                             JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                             WHERE ptp.PersonID = fc.SuppPersonID AND ptp.KnessetNum = fc.KnessetNum
                             ORDER BY ptp.StartDate DESC LIMIT 1),
                            (SELECT f.FactionID 
                             FROM KNS_PersonToPosition ptp 
                             JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                             WHERE ptp.PersonID = fc.SuppPersonID
                             ORDER BY ptp.KnessetNum DESC, ptp.StartDate DESC LIMIT 1)
                        ) as FactionID
                    FROM FactionCollaborations fc
                ),
                FactionTotalBills AS (
                    SELECT 
                        f.FactionID,
                        f.Name as FactionName,
                        COUNT(DISTINCT bi.BillID) as TotalBills
                    FROM KNS_Faction f
                    JOIN KNS_PersonToPosition ptp ON f.FactionID = ptp.FactionID
                    JOIN KNS_BillInitiator bi ON ptp.PersonID = bi.PersonID AND bi.Ordinal = 1
                    JOIN KNS_Bill b ON bi.BillID = b.BillID AND b.KnessetNum = ptp.KnessetNum
                    WHERE b.KnessetNum IS NOT NULL
                        AND {filters["knesset_condition"]}
                    GROUP BY f.FactionID, f.Name
                )
                SELECT 
                    main_pf.FactionID as MainFactionID,
                    supp_pf.FactionID as SupporterFactionID,
                    COUNT(DISTINCT fc.BillID) as CollaborationCount,
                    main_f.Name as MainFactionName,
                    supp_f.Name as SupporterFactionName,
                    COALESCE(main_ufs.CoalitionStatus, 'Unknown') as MainCoalitionStatus,
                    COALESCE(supp_ufs.CoalitionStatus, 'Unknown') as SupporterCoalitionStatus,
                    COALESCE(main_ftb.TotalBills, 0) as MainFactionTotalBills,
                    COALESCE(supp_ftb.TotalBills, 0) as SupporterFactionTotalBills
                FROM FactionCollaborations fc
                JOIN PersonFactions main_pf ON fc.MainPersonID = main_pf.PersonID
                JOIN PersonFactions supp_pf ON fc.SuppPersonID = supp_pf.PersonID
                JOIN KNS_Faction main_f ON main_pf.FactionID = main_f.FactionID
                JOIN KNS_Faction supp_f ON supp_pf.FactionID = supp_f.FactionID
                LEFT JOIN UserFactionCoalitionStatus main_ufs ON main_f.FactionID = main_ufs.FactionID AND fc.KnessetNum = main_ufs.KnessetNum
                LEFT JOIN UserFactionCoalitionStatus supp_ufs ON supp_f.FactionID = supp_ufs.FactionID AND fc.KnessetNum = supp_ufs.KnessetNum
                LEFT JOIN FactionTotalBills main_ftb ON main_f.FactionID = main_ftb.FactionID
                LEFT JOIN FactionTotalBills supp_ftb ON supp_f.FactionID = supp_ftb.FactionID
                WHERE main_pf.FactionID IS NOT NULL
                    AND supp_pf.FactionID IS NOT NULL
                    AND main_pf.FactionID <> supp_pf.FactionID
                GROUP BY main_pf.FactionID, supp_pf.FactionID, main_f.Name, supp_f.Name, main_ufs.CoalitionStatus, supp_ufs.CoalitionStatus, main_ftb.TotalBills, supp_ftb.TotalBills
                HAVING COUNT(DISTINCT fc.BillID) >= {min_collaborations}
                ORDER BY CollaborationCount DESC
                """

                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(f"No faction collaboration data found for '{filters['knesset_title']}' with minimum {min_collaborations} collaborations.")
                    return None

                # Create network visualization
                return self._create_faction_network_chart(df, filters['knesset_title'])

        except Exception as e:
            self.logger.error(f"Error generating faction collaboration network: {e}", exc_info=True)
            st.error(f"Could not generate faction collaboration network: {e}")
            return None

    def plot_faction_coalition_breakdown(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        min_collaborations: int = 5,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate faction collaboration breakdown chart showing Coalition vs Opposition collaboration percentages."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b", **kwargs)

        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(con, ["KNS_Bill", "KNS_BillInitiator", "KNS_PersonToPosition", "KNS_Faction", "UserFactionCoalitionStatus"]):
                    return None

                # Get faction collaboration breakdown data
                query = f"""
                WITH FactionCollaborations AS (
                    SELECT 
                        main_f.FactionID as MainFactionID,
                        main_f.Name as MainFactionName,
                        COALESCE(main_ufs.CoalitionStatus, 'Unknown') as MainCoalitionStatus,
                        COALESCE(supp_ufs.CoalitionStatus, 'Unknown') as SupporterCoalitionStatus,
                        COUNT(DISTINCT main.BillID) as CollaborationCount
                    FROM KNS_BillInitiator main
                    JOIN KNS_Bill b ON main.BillID = b.BillID
                    JOIN KNS_BillInitiator supp ON main.BillID = supp.BillID 
                    LEFT JOIN KNS_PersonToPosition main_ptp ON main.PersonID = main_ptp.PersonID AND b.KnessetNum = main_ptp.KnessetNum
                    LEFT JOIN KNS_PersonToPosition supp_ptp ON supp.PersonID = supp_ptp.PersonID AND b.KnessetNum = supp_ptp.KnessetNum
                    LEFT JOIN KNS_Faction main_f ON main_ptp.FactionID = main_f.FactionID
                    LEFT JOIN KNS_Faction supp_f ON supp_ptp.FactionID = supp_f.FactionID
                    LEFT JOIN UserFactionCoalitionStatus main_ufs ON main_f.FactionID = main_ufs.FactionID AND b.KnessetNum = main_ufs.KnessetNum
                    LEFT JOIN UserFactionCoalitionStatus supp_ufs ON supp_f.FactionID = supp_ufs.FactionID AND b.KnessetNum = supp_ufs.KnessetNum
                    WHERE main.Ordinal = 1 
                        AND supp.Ordinal > 1
                        AND main_f.FactionID IS NOT NULL
                        AND supp_f.FactionID IS NOT NULL
                        AND main_f.FactionID <> supp_f.FactionID
                        AND supp_ufs.CoalitionStatus IS NOT NULL
                        AND supp_ufs.CoalitionStatus IN ('Coalition', 'Opposition')
                        AND b.KnessetNum IS NOT NULL
                        AND {filters["knesset_condition"]}
                    GROUP BY main_f.FactionID, main_f.Name, main_ufs.CoalitionStatus, supp_ufs.CoalitionStatus
                )
                SELECT 
                    MainFactionID,
                    MainFactionName,
                    MainCoalitionStatus,
                    SUM(CollaborationCount) as TotalCollaborations,
                    SUM(CASE WHEN SupporterCoalitionStatus = 'Coalition' THEN CollaborationCount ELSE 0 END) as CoalitionCollaborations,
                    SUM(CASE WHEN SupporterCoalitionStatus = 'Opposition' THEN CollaborationCount ELSE 0 END) as OppositionCollaborations,
                    ROUND(
                        100.0 * SUM(CASE WHEN SupporterCoalitionStatus = 'Coalition' THEN CollaborationCount ELSE 0 END) / 
                        SUM(CollaborationCount), 1
                    ) as CoalitionPercentage,
                    ROUND(
                        100.0 * SUM(CASE WHEN SupporterCoalitionStatus = 'Opposition' THEN CollaborationCount ELSE 0 END) / 
                        SUM(CollaborationCount), 1
                    ) as OppositionPercentage
                FROM FactionCollaborations
                GROUP BY MainFactionID, MainFactionName, MainCoalitionStatus
                HAVING SUM(CollaborationCount) >= {min_collaborations}
                ORDER BY TotalCollaborations DESC
                """

                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(f"No faction collaboration breakdown data found for '{filters['knesset_title']}' with minimum {min_collaborations} collaborations.")
                    return None

                # Create stacked bar chart visualization
                return self._create_faction_breakdown_chart(df, filters['knesset_title'])

        except Exception as e:
            self.logger.error(f"Error generating faction collaboration breakdown: {e}", exc_info=True)
            st.error(f"Could not generate faction collaboration breakdown: {e}")
            return None

    def _create_network_chart(
        self,
        df: pd.DataFrame,
        node_id_col: str,
        node_name_col: str,
        node_group_col: str,
        edge_source_col: str,
        edge_target_col: str,
        edge_weight_col: str,
        title: str,
        node_size_col: Optional[str] = None
    ) -> go.Figure:
        """Create a network chart from collaboration data."""
        
        # Create nodes list - handle the actual column names from our queries
        main_node_cols = [edge_source_col]
        supp_node_cols = [edge_target_col]
        
        # Add name and group columns based on actual data structure
        if f'Main{node_name_col}' in df.columns:
            main_node_cols.append(f'Main{node_name_col}')
            main_name_col = f'Main{node_name_col}'
        else:
            # Fallback to direct column names
            main_name_col = node_name_col
            if main_name_col in df.columns:
                main_node_cols.append(main_name_col)
        
        if f'Main{node_group_col}' in df.columns:
            main_node_cols.append(f'Main{node_group_col}')
            main_group_col = f'Main{node_group_col}'
        else:
            main_group_col = node_group_col
            if main_group_col in df.columns:
                main_node_cols.append(main_group_col)
        
        if f'Supporter{node_name_col}' in df.columns:
            supp_node_cols.append(f'Supporter{node_name_col}')
            supp_name_col = f'Supporter{node_name_col}'
        else:
            supp_name_col = node_name_col
            if supp_name_col in df.columns:
                supp_node_cols.append(supp_name_col)
        
        if f'Supporter{node_group_col}' in df.columns:
            supp_node_cols.append(f'Supporter{node_group_col}')
            supp_group_col = f'Supporter{node_group_col}'
        else:
            supp_group_col = node_group_col
            if supp_group_col in df.columns:
                supp_node_cols.append(supp_group_col)
        
        # Create main nodes DataFrame
        main_nodes = df[main_node_cols].copy()
        main_nodes = main_nodes.rename(columns={
            edge_source_col: node_id_col,
            main_name_col: node_name_col,
            main_group_col: node_group_col
        })
        
        # Create supporter nodes DataFrame
        supp_nodes = df[supp_node_cols].copy()
        supp_nodes = supp_nodes.rename(columns={
            edge_target_col: node_id_col,
            supp_name_col: node_name_col,
            supp_group_col: node_group_col
        })
        
        if node_size_col:
            main_size_col = f'Main{node_size_col}' if f'Main{node_size_col}' in df.columns else node_size_col
            supp_size_col = f'Supporter{node_size_col}' if f'Supporter{node_size_col}' in df.columns else node_size_col
            
            if main_size_col in df.columns:
                main_nodes[node_size_col] = df[main_size_col]
            if supp_size_col in df.columns:
                supp_nodes[node_size_col] = df[supp_size_col]
        
        all_nodes = pd.concat([main_nodes, supp_nodes]).drop_duplicates(subset=[node_id_col])
        
        # Create edges
        edges = df[[edge_source_col, edge_target_col, edge_weight_col]].copy()
        
        # Generate layout using simple circular/force-directed approach
        n_nodes = len(all_nodes)
        angles = np.linspace(0, 2*np.pi, n_nodes, endpoint=False)
        radius = max(10, n_nodes / 3)
        
        node_positions = {}
        for i, (_, node) in enumerate(all_nodes.iterrows()):
            node_id = node[node_id_col]
            x = radius * np.cos(angles[i])
            y = radius * np.sin(angles[i])
            node_positions[node_id] = (x, y)
        
        # Create traces
        fig = go.Figure()
        
        # Add edges
        edge_x = []
        edge_y = []
        edge_info = []
        
        for _, edge in edges.iterrows():
            source_pos = node_positions[edge[edge_source_col]]
            target_pos = node_positions[edge[edge_target_col]]
            
            edge_x.extend([source_pos[0], target_pos[0], None])
            edge_y.extend([source_pos[1], target_pos[1], None])
            edge_info.append(f"{edge[edge_weight_col]} collaborations")
        
        fig.add_trace(go.Scatter(
            x=edge_x, y=edge_y,
            mode='lines',
            line=dict(width=1, color='#888'),
            hoverinfo='none',
            showlegend=False
        ))
        
        # Add nodes
        unique_groups = all_nodes[node_group_col].unique()
        colors = px.colors.qualitative.Set3[:len(unique_groups)]
        color_map = dict(zip(unique_groups, colors))
        
        for group in unique_groups:
            group_nodes = all_nodes[all_nodes[node_group_col] == group]
            
            node_x = [node_positions[node_id][0] for node_id in group_nodes[node_id_col]]
            node_y = [node_positions[node_id][1] for node_id in group_nodes[node_id_col]]
            
            node_sizes = [10] * len(group_nodes)
            if node_size_col and node_size_col in group_nodes.columns:
                max_size = group_nodes[node_size_col].max()
                if max_size > 0:
                    node_sizes = [max(8, min(30, 8 + (size / max_size * 20))) for size in group_nodes[node_size_col]]
            
            hover_text = [f"{name}<br>{group}<br>Connections: {len(edges[(edges[edge_source_col] == node_id) | (edges[edge_target_col] == node_id)])}"
                         for node_id, name in zip(group_nodes[node_id_col], group_nodes[node_name_col])]
            
            fig.add_trace(go.Scatter(
                x=node_x, y=node_y,
                mode='markers+text',
                marker=dict(size=node_sizes, color=color_map[group], line=dict(width=2, color='white')),
                text=group_nodes[node_name_col],
                textposition="middle center",
                textfont=dict(size=8),
                hovertext=hover_text,
                hoverinfo='text',
                name=group,
                showlegend=True
            ))
        
        fig.update_layout(
            title=title,
            title_x=0.5,
            showlegend=True,
            hovermode='closest',
            margin=dict(b=20,l=5,r=5,t=40),
            annotations=[
                dict(
                    text=f"Network shows collaborations with {edges[edge_weight_col].min()}+ bills together",
                    showarrow=False,
                    xref="paper", yref="paper",
                    x=0.005, y=-0.002,
                    xanchor='left', yanchor='bottom',
                    font=dict(size=12)
                )
            ],
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            height=700
        )
        
        return fig

    def _create_coalition_network_chart(self, df: pd.DataFrame, title_suffix: str) -> go.Figure:
        """Create specialized network chart for coalition/opposition collaboration."""
        
        fig = go.Figure()
        
        # Group by coalition status pairs
        coalition_pairs = df.groupby(['MainCoalitionStatus', 'SupporterCoalitionStatus'])['CollaborationCount'].sum().reset_index()
        
        # Create a simplified network showing coalition-opposition connections
        colors = {'Coalition': '#1f77b4', 'Opposition': '#ff7f0e'}
        
        # Position coalition and opposition groups
        coalition_x, coalition_y = -2, 0
        opposition_x, opposition_y = 2, 0
        
        # Add coalition and opposition nodes
        for status, (x, y) in [('Coalition', (coalition_x, coalition_y)), ('Opposition', (opposition_x, opposition_y))]:
            fig.add_trace(go.Scatter(
                x=[x], y=[y],
                mode='markers+text',
                marker=dict(size=50, color=colors[status], line=dict(width=3, color='white')),
                text=[status],
                textposition="middle center",
                textfont=dict(size=14, color='white'),
                name=status,
                showlegend=True
            ))
        
        # Add collaboration connections
        total_collaborations = df['CollaborationCount'].sum()
        
        for _, row in coalition_pairs.iterrows():
            main_status = row['MainCoalitionStatus']
            supp_status = row['SupporterCoalitionStatus']
            count = row['CollaborationCount']
            
            if main_status != supp_status:  # Cross-party collaboration
                main_pos = (coalition_x, coalition_y) if main_status == 'Coalition' else (opposition_x, opposition_y)
                supp_pos = (coalition_x, coalition_y) if supp_status == 'Coalition' else (opposition_x, opposition_y)
                
                # Add connection line
                fig.add_trace(go.Scatter(
                    x=[main_pos[0], supp_pos[0]], 
                    y=[main_pos[1], supp_pos[1]],
                    mode='lines',
                    line=dict(width=max(2, count/5), color='red'),
                    hovertemplate=f'Cross-party collaboration<br>{count} bills<extra></extra>',
                    showlegend=False
                ))
        
        # Add faction details as annotations
        annotation_text = []
        for _, row in df.head(10).iterrows():  # Show top 10 collaborations
            annotation_text.append(
                f"• {row['MainFactionName']} ({row['MainCoalitionStatus']}) → "
                f"{row['SupporterFactionName']} ({row['SupporterCoalitionStatus']}): "
                f"{row['CollaborationCount']} bills"
            )
        
        fig.update_layout(
            title=f"<b>Coalition/Opposition Cross-Party Collaboration<br>{title_suffix}</b>",
            title_x=0.5,
            showlegend=True,
            hovermode='closest',
            margin=dict(b=20,l=5,r=5,t=80),
            annotations=[
                dict(
                    text="<br>".join(annotation_text[:5]),  # Show first 5 in annotation
                    showarrow=False,
                    xref="paper", yref="paper",
                    x=0.02, y=0.98,
                    xanchor='left', yanchor='top',
                    font=dict(size=10),
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="gray",
                    borderwidth=1
                )
            ],
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-4, 4]),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-2, 2]),
            height=600
        )
        
        return fig

    def _generate_force_directed_layout(self, nodes_df: pd.DataFrame, edges_df: pd.DataFrame) -> dict:
        """Generate force-directed layout positions for network nodes."""
        import random
        
        # Initialize random positions
        positions = {}
        for _, node in nodes_df.iterrows():
            person_id = node['PersonID']
            positions[person_id] = [
                random.uniform(-50, 50),
                random.uniform(-50, 50)
            ]
        
        # Create adjacency list for connections
        connections = {}
        for _, node in nodes_df.iterrows():
            connections[node['PersonID']] = []
        
        for _, edge in edges_df.iterrows():
            main_id = edge['MainInitiatorID']
            supp_id = edge['SupporterID']
            if main_id in connections and supp_id in connections:
                connections[main_id].append(supp_id)
                connections[supp_id].append(main_id)
        
        # Force-directed algorithm parameters
        iterations = 100
        k = 30  # Optimal distance between nodes
        area = 10000  # Total area
        dt = 0.1  # Time step
        
        for iteration in range(iterations):
            # Calculate forces
            forces = {}
            for person_id in positions:
                forces[person_id] = [0.0, 0.0]
            
            # Repulsive forces (all nodes repel each other)
            node_ids = list(positions.keys())
            for i, id1 in enumerate(node_ids):
                for id2 in node_ids[i+1:]:
                    dx = positions[id1][0] - positions[id2][0]
                    dy = positions[id1][1] - positions[id2][1]
                    distance = max(np.sqrt(dx*dx + dy*dy), 0.1)
                    
                    # Repulsive force magnitude
                    force_mag = k * k / distance
                    
                    # Apply forces
                    fx = force_mag * dx / distance
                    fy = force_mag * dy / distance
                    
                    forces[id1][0] += fx
                    forces[id1][1] += fy
                    forces[id2][0] -= fx
                    forces[id2][1] -= fy
            
            # Attractive forces (connected nodes attract each other)
            for id1 in connections:
                for id2 in connections[id1]:
                    dx = positions[id2][0] - positions[id1][0]
                    dy = positions[id2][1] - positions[id1][1]
                    distance = max(np.sqrt(dx*dx + dy*dy), 0.1)
                    
                    # Attractive force magnitude
                    force_mag = distance * distance / k
                    
                    # Apply forces
                    fx = force_mag * dx / distance
                    fy = force_mag * dy / distance
                    
                    forces[id1][0] += fx
                    forces[id1][1] += fy
            
            # Update positions
            for person_id in positions:
                # Limit force magnitude to prevent instability
                force_magnitude = np.sqrt(forces[person_id][0]**2 + forces[person_id][1]**2)
                if force_magnitude > 0:
                    max_displacement = min(force_magnitude * dt, 10)
                    positions[person_id][0] += (forces[person_id][0] / force_magnitude) * max_displacement
                    positions[person_id][1] += (forces[person_id][1] / force_magnitude) * max_displacement
        
        return positions

    def _create_better_network_layout(self, nodes_df: pd.DataFrame, edges_df: pd.DataFrame) -> dict:
        """Create an improved network layout that clusters connected nodes."""
        import random
        import math
        
        # Get collaboration strength for each person
        collaboration_strength = {}
        for _, node in nodes_df.iterrows():
            person_id = node['PersonID']
            connections = edges_df[(edges_df['MainInitiatorID'] == person_id) | (edges_df['SupporterID'] == person_id)]
            collaboration_strength[person_id] = len(connections)
        
        # Group by faction for initial positioning
        faction_groups = {}
        for _, node in nodes_df.iterrows():
            faction = node['Faction']
            if faction not in faction_groups:
                faction_groups[faction] = []
            faction_groups[faction].append(node['PersonID'])
        
        positions = {}
        faction_centers = {}
        
        # Position faction centers in a circle
        num_factions = len(faction_groups)
        for i, faction in enumerate(faction_groups.keys()):
            angle = 2 * math.pi * i / num_factions
            center_x = 80 * math.cos(angle)  # Larger radius for better spacing
            center_y = 80 * math.sin(angle)
            faction_centers[faction] = (center_x, center_y)
        
        # Position nodes within their faction clusters
        for faction, member_ids in faction_groups.items():
            center_x, center_y = faction_centers[faction]
            
            if len(member_ids) == 1:
                positions[member_ids[0]] = (center_x, center_y)
            else:
                # Arrange faction members in a small circle around faction center
                for j, person_id in enumerate(member_ids):
                    member_angle = 2 * math.pi * j / len(member_ids)
                    # Distance from center based on collaboration activity
                    distance = 15 + (collaboration_strength.get(person_id, 0) * 2)
                    x = center_x + distance * math.cos(member_angle)
                    y = center_y + distance * math.sin(member_angle)
                    positions[person_id] = (x, y)
        
        return positions

    def _generate_force_directed_layout_factions(self, factions_df: pd.DataFrame, edges_df: pd.DataFrame) -> dict:
        """Generate force-directed layout positions for faction network nodes."""
        import random
        
        # Initialize random positions
        positions = {}
        for _, faction in factions_df.iterrows():
            faction_id = faction['FactionID']
            positions[faction_id] = [
                random.uniform(-40, 40),
                random.uniform(-40, 40)
            ]
        
        # Create adjacency list for connections
        connections = {}
        for _, faction in factions_df.iterrows():
            connections[faction['FactionID']] = []
        
        for _, edge in edges_df.iterrows():
            main_id = edge['MainFactionID']
            supp_id = edge['SupporterFactionID']
            if main_id in connections and supp_id in connections:
                connections[main_id].append(supp_id)
                connections[supp_id].append(main_id)
        
        # Force-directed algorithm parameters (adjusted for fewer nodes)
        iterations = 80
        k = 25  # Optimal distance between nodes
        dt = 0.15  # Time step
        
        for iteration in range(iterations):
            # Calculate forces
            forces = {}
            for faction_id in positions:
                forces[faction_id] = [0.0, 0.0]
            
            # Repulsive forces (all nodes repel each other)
            faction_ids = list(positions.keys())
            for i, id1 in enumerate(faction_ids):
                for id2 in faction_ids[i+1:]:
                    dx = positions[id1][0] - positions[id2][0]
                    dy = positions[id1][1] - positions[id2][1]
                    distance = max(np.sqrt(dx*dx + dy*dy), 0.1)
                    
                    # Repulsive force magnitude
                    force_mag = k * k / distance
                    
                    # Apply forces
                    fx = force_mag * dx / distance
                    fy = force_mag * dy / distance
                    
                    forces[id1][0] += fx
                    forces[id1][1] += fy
                    forces[id2][0] -= fx
                    forces[id2][1] -= fy
            
            # Attractive forces (connected nodes attract each other)
            for id1 in connections:
                for id2 in connections[id1]:
                    dx = positions[id2][0] - positions[id1][0]
                    dy = positions[id2][1] - positions[id1][1]
                    distance = max(np.sqrt(dx*dx + dy*dy), 0.1)
                    
                    # Attractive force magnitude
                    force_mag = distance * distance / k
                    
                    # Apply forces
                    fx = force_mag * dx / distance
                    fy = force_mag * dy / distance
                    
                    forces[id1][0] += fx
                    forces[id1][1] += fy
            
            # Update positions
            for faction_id in positions:
                # Limit force magnitude to prevent instability
                force_magnitude = np.sqrt(forces[faction_id][0]**2 + forces[faction_id][1]**2)
                if force_magnitude > 0:
                    max_displacement = min(force_magnitude * dt, 8)
                    positions[faction_id][0] += (forces[faction_id][0] / force_magnitude) * max_displacement
                    positions[faction_id][1] += (forces[faction_id][1] / force_magnitude) * max_displacement
        
        return positions

    def _create_faction_layout(self, factions_df: pd.DataFrame, edges_df: pd.DataFrame) -> dict:
        """Create clear faction layout with coalition/opposition separation."""
        import math
        
        # Separate by coalition status
        coalition_factions = []
        opposition_factions = []
        unknown_factions = []
        
        for _, faction in factions_df.iterrows():
            status = faction['Status']
            if status == 'Coalition':
                coalition_factions.append(faction)
            elif status == 'Opposition':
                opposition_factions.append(faction)
            else:
                unknown_factions.append(faction)
        
        positions = {}
        
        # Position coalition factions on the left side
        if coalition_factions:
            for i, faction in enumerate(coalition_factions):
                angle = math.pi * i / max(1, len(coalition_factions) - 1) - math.pi/2  # Left semicircle
                x = -60 + 40 * math.cos(angle)
                y = 60 * math.sin(angle)
                positions[faction['FactionID']] = (x, y)
        
        # Position opposition factions on the right side  
        if opposition_factions:
            for i, faction in enumerate(opposition_factions):
                angle = math.pi * i / max(1, len(opposition_factions) - 1) + math.pi/2  # Right semicircle
                x = 60 + 40 * math.cos(angle)
                y = 60 * math.sin(angle)
                positions[faction['FactionID']] = (x, y)
        
        # Position unknown factions at the bottom
        if unknown_factions:
            for i, faction in enumerate(unknown_factions):
                x = -30 + (60 * i / max(1, len(unknown_factions) - 1)) if len(unknown_factions) > 1 else 0
                y = -80
                positions[faction['FactionID']] = (x, y)
        
        return positions

    def _create_mk_network_chart(self, df: pd.DataFrame, title_suffix: str) -> go.Figure:
        """Create MK collaboration network chart with force-directed layout and interactive highlighting."""
        
        try:
            # Extract unique nodes with safe string conversion
            main_nodes = df[['MainInitiatorID', 'MainInitiatorName', 'MainInitiatorFaction', 'MainInitiatorTotalBills']].copy()
            main_nodes.columns = ['PersonID', 'Name', 'Faction', 'TotalBills']
            main_nodes['Name'] = main_nodes['Name'].astype(str)
            main_nodes['Faction'] = main_nodes['Faction'].astype(str)
            
            supp_nodes = df[['SupporterID', 'SupporterName', 'SupporterFaction', 'SupporterTotalBills']].copy()
            supp_nodes.columns = ['PersonID', 'Name', 'Faction', 'TotalBills']
            supp_nodes['Name'] = supp_nodes['Name'].astype(str)
            supp_nodes['Faction'] = supp_nodes['Faction'].astype(str)
            
            all_nodes = pd.concat([main_nodes, supp_nodes]).drop_duplicates(subset=['PersonID'])
            
        except Exception as e:
            self.logger.error(f"Error processing node data: {e}")
            # Return empty chart on error
            fig = go.Figure()
            fig.add_annotation(text="Error processing network data", 
                             xref="paper", yref="paper", x=0.5, y=0.5,
                             showarrow=False, font=dict(size=16))
            return fig
        
        # Generate improved network layout with better spacing
        node_positions = self._create_better_network_layout(all_nodes, df)
        
        # Prepare faction colors
        unique_factions = all_nodes['Faction'].unique()
        colors = px.colors.qualitative.Set3[:len(unique_factions)]
        color_map = dict(zip(unique_factions, colors))
        
        # Create the interactive network visualization
        fig = go.Figure()
        
        # Add ALL edges as a single trace (cleaner approach)
        edge_x = []
        edge_y = []
        edge_info = []
        
        for _, edge in df.iterrows():
            source_pos = node_positions[edge['MainInitiatorID']]
            target_pos = node_positions[edge['SupporterID']]
            
            edge_x.extend([source_pos[0], target_pos[0], None])
            edge_y.extend([source_pos[1], target_pos[1], None])
            edge_info.append(f"{edge['CollaborationCount']} collaborations")
        
        # Single edge trace
        fig.add_trace(go.Scatter(
            x=edge_x, y=edge_y,
            mode='lines',
            line=dict(width=0.8, color='rgba(100,100,100,0.4)'),
            hoverinfo='none',
            showlegend=False,
            name='connections'
        ))
        
        # Add nodes grouped by faction for better legend
        max_bills = all_nodes['TotalBills'].max() if not all_nodes['TotalBills'].empty else 1
        
        for faction in unique_factions:
            try:
                faction_nodes = all_nodes[all_nodes['Faction'] == faction]
                
                node_x = []
                node_y = []
                node_sizes = []
                hover_texts = []
                node_names = []
                
                for _, node in faction_nodes.iterrows():
                    person_id = node['PersonID']
                    pos = node_positions[person_id]
                    
                    # MUCH larger node sizes for visibility
                    node_size = max(20, min(80, 20 + (node['TotalBills'] / max_bills * 60)))
                    
                    # Get connections for this node
                    connections = df[(df['MainInitiatorID'] == person_id) | (df['SupporterID'] == person_id)]
                    collaboration_count = len(connections)
                    
                    node_x.append(pos[0])
                    node_y.append(pos[1])
                    node_sizes.append(node_size)
                    node_names.append(node['Name'])
                    
                    hover_text = (f"<b>{node['Name']}</b><br>"
                                f"Faction: {faction}<br>"
                                f"Total Bills: {node['TotalBills']}<br>"
                                f"Collaborations: {collaboration_count}")
                    hover_texts.append(hover_text)
                
                # Create faction trace with enhanced visibility
                fig.add_trace(go.Scatter(
                    x=node_x, 
                    y=node_y,
                    mode='markers+text',
                    marker=dict(
                        size=node_sizes,
                        color=color_map.get(faction, '#808080'),
                        line=dict(width=3, color='white'),
                        opacity=0.9
                    ),
                    text=node_names,
                    textposition="middle center",
                    textfont=dict(size=10, color='black', family="Arial Black"),
                    hovertext=hover_texts,
                    hoverinfo='text',
                    name=str(faction),
                    showlegend=True,
                    legendgroup=faction
                ))
                
            except Exception as e:
                self.logger.error(f"Error processing faction '{faction}': {e}")
                continue
        
        fig.update_layout(
            title=f"<b>🔗 MK Collaboration Network - Enhanced Layout<br>{title_suffix}</b>",
            title_x=0.5,
            showlegend=True,
            hovermode='closest',
            margin=dict(b=40, l=40, r=40, t=80),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-150, 150]),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-150, 150]),
            height=900,
            width=900,
            plot_bgcolor='rgba(240,240,240,0.1)',
            annotations=[],
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.05,
                font=dict(size=10)
            )
        )
        
        return fig

    def _create_faction_network_chart(self, df: pd.DataFrame, title_suffix: str) -> go.Figure:
        """Create faction collaboration network chart with force-directed layout and interactive features."""
        
        try:
            # Extract unique factions with safe string conversion
            main_factions = df[['MainFactionID', 'MainFactionName', 'MainCoalitionStatus']].copy()
            main_factions.columns = ['FactionID', 'Name', 'Status']
            main_factions['Name'] = main_factions['Name'].astype(str)
            main_factions['Status'] = main_factions['Status'].astype(str)
            
            supp_factions = df[['SupporterFactionID', 'SupporterFactionName', 'SupporterCoalitionStatus']].copy()
            supp_factions.columns = ['FactionID', 'Name', 'Status']
            supp_factions['Name'] = supp_factions['Name'].astype(str)
            supp_factions['Status'] = supp_factions['Status'].astype(str)
            
            all_factions = pd.concat([main_factions, supp_factions]).drop_duplicates(subset=['FactionID'])
            
            # Add total bills for proper node sizing (not just collaboration count)
            faction_total_bills = {}
            for _, row in df.iterrows():
                # Get total bills for main faction
                main_faction_id = row['MainFactionID']
                if main_faction_id not in faction_total_bills:
                    faction_total_bills[main_faction_id] = row['MainFactionTotalBills']
                
                # Get total bills for supporter faction  
                supp_faction_id = row['SupporterFactionID']
                if supp_faction_id not in faction_total_bills:
                    faction_total_bills[supp_faction_id] = row['SupporterFactionTotalBills']
            
            # Also calculate collaboration count for hover info
            faction_collaboration_counts = {}
            for _, faction in all_factions.iterrows():
                faction_id = faction['FactionID']
                collaborations = df[(df['MainFactionID'] == faction_id) | (df['SupporterFactionID'] == faction_id)]
                faction_collaboration_counts[faction_id] = len(collaborations)
            
            all_factions['TotalBills'] = all_factions['FactionID'].map(faction_total_bills).fillna(0)
            all_factions['CollaborationCount'] = all_factions['FactionID'].map(faction_collaboration_counts)
            
        except Exception as e:
            self.logger.error(f"Error processing faction data: {e}")
            # Return empty chart on error
            fig = go.Figure()
            fig.add_annotation(text="Error processing faction network data", 
                             xref="paper", yref="paper", x=0.5, y=0.5,
                             showarrow=False, font=dict(size=16))
            return fig
        
        # Generate simplified faction layout
        node_positions = self._create_faction_layout(all_factions, df)
        
        # Prepare status colors
        status_colors = {
            'Coalition': '#1f77b4',
            'Opposition': '#ff7f0e', 
            'Unknown': '#808080'
        }
        
        fig = go.Figure()
        
        # Add edges with much more visible styling
        edge_x = []
        edge_y = []
        
        for _, edge in df.iterrows():
            source_pos = node_positions[edge['MainFactionID']]
            target_pos = node_positions[edge['SupporterFactionID']]
            
            edge_x.extend([source_pos[0], target_pos[0], None])
            edge_y.extend([source_pos[1], target_pos[1], None])
        
        # Single edge trace with better visibility
        fig.add_trace(go.Scatter(
            x=edge_x, y=edge_y,
            mode='lines',
            line=dict(width=2, color='rgba(50,50,50,0.6)'),
            hoverinfo='none',
            showlegend=False,
            name='collaborations'
        ))
        
        # Add faction nodes grouped by status for better visualization
        max_bills = all_factions['TotalBills'].max() if not all_factions['TotalBills'].empty else 1
        
        # Group factions by status and create separate traces
        for status in ['Coalition', 'Opposition', 'Unknown']:
            status_factions = all_factions[all_factions['Status'] == status]
            if status_factions.empty:
                continue
                
            node_x = []
            node_y = []
            node_sizes = []
            hover_texts = []
            node_names = []
            
            for _, faction in status_factions.iterrows():
                try:
                    faction_id = faction['FactionID']
                    pos = node_positions[faction_id]
                    
                    # Node sizes based on total bills initiated by faction (avoiding double-counting)
                    node_size = max(30, min(100, 30 + (faction['TotalBills'] / max_bills * 70)))
                    
                    # Get detailed collaboration info
                    collaborations = df[(df['MainFactionID'] == faction_id) | (df['SupporterFactionID'] == faction_id)]
                    partner_factions = set()
                    for _, collab in collaborations.iterrows():
                        if collab['MainFactionID'] == faction_id:
                            partner_factions.add(collab['SupporterFactionName'])
                        else:
                            partner_factions.add(collab['MainFactionName'])
                    
                    node_x.append(pos[0])
                    node_y.append(pos[1])
                    node_sizes.append(node_size)
                    node_names.append(faction['Name'])
                    
                    hover_text = (f"<b>{faction['Name']}</b><br>"
                                f"Status: {status}<br>"
                                f"Total Bills: {int(faction['TotalBills'])}<br>"
                                f"Collaborations: {faction['CollaborationCount']}<br>"
                                f"Partner Factions: {len(partner_factions)}")
                    hover_texts.append(hover_text)
                    
                except Exception as e:
                    self.logger.error(f"Error processing faction '{faction_id}': {e}")
                    continue
            
            if node_x:  # Only add trace if we have data
                fig.add_trace(go.Scatter(
                    x=node_x, 
                    y=node_y,
                    mode='markers+text',
                    marker=dict(
                        size=node_sizes,
                        color=status_colors.get(status, '#808080'),
                        line=dict(width=4, color='white'),
                        opacity=0.9
                    ),
                    text=node_names,
                    textposition="middle center",
                    textfont=dict(size=12, color='black', family="Arial Black"),
                    hovertext=hover_texts,
                    hoverinfo='text',
                    name=status,
                    showlegend=True,
                    legendgroup=status
                ))
        
        fig.update_layout(
            title=f"<b>🏛️ Faction Network - Coalition vs Opposition Layout<br>{title_suffix}</b>",
            title_x=0.5,
            showlegend=True,
            hovermode='closest',
            margin=dict(b=40, l=40, r=40, t=80),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-140, 140]),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-120, 120]),
            height=900,
            width=900,
            plot_bgcolor='rgba(240,240,240,0.1)',
            annotations=[],
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.05,
                font=dict(size=12)
            )
        )
        
        return fig

    def _create_faction_breakdown_chart(self, df: pd.DataFrame, title_suffix: str) -> go.Figure:
        """Create stacked bar chart showing faction collaboration breakdown by Coalition vs Opposition."""
        
        # Sort by total collaborations descending
        df_sorted = df.sort_values('TotalCollaborations', ascending=True)  # Ascending for horizontal bar
        
        # Prepare data
        faction_names = df_sorted['MainFactionName'].tolist()
        coalition_pct = df_sorted['CoalitionPercentage'].tolist()
        opposition_pct = df_sorted['OppositionPercentage'].tolist()
        total_collaborations = df_sorted['TotalCollaborations'].tolist()
        faction_status = df_sorted['MainCoalitionStatus'].tolist()
        
        # Create horizontal stacked bar chart
        fig = go.Figure()
        
        # Add Coalition bars
        fig.add_trace(go.Bar(
            name='Coalition Collaborations',
            y=faction_names,
            x=coalition_pct,
            orientation='h',
            marker=dict(color='#1f77b4', opacity=0.8),
            hovertemplate='<b>%{y}</b><br>Coalition: %{x}%<br>Count: %{customdata}<extra></extra>',
            customdata=df_sorted['CoalitionCollaborations'].tolist()
        ))
        
        # Add Opposition bars
        fig.add_trace(go.Bar(
            name='Opposition Collaborations',
            y=faction_names,
            x=opposition_pct,
            orientation='h',
            marker=dict(color='#ff7f0e', opacity=0.8),
            hovertemplate='<b>%{y}</b><br>Opposition: %{x}%<br>Count: %{customdata}<extra></extra>',
            customdata=df_sorted['OppositionCollaborations'].tolist()
        ))
        
        # Add total collaboration annotations
        for i, (faction, total, coal_pct, opp_pct, status) in enumerate(zip(faction_names, total_collaborations, coalition_pct, opposition_pct, faction_status)):
            fig.add_annotation(
                x=102,  # Just outside the 100% mark
                y=i,
                text=f"Total: {int(total)}",
                showarrow=False,
                font=dict(size=10, color='black'),
                xanchor='left'
            )
        
        fig.update_layout(
            title=f"<b>📊 Faction Collaboration Breakdown - Coalition vs Opposition<br>{title_suffix}</b>",
            title_x=0.5,
            title_y=0.98,  # Move title higher
            xaxis=dict(
                title="Percentage of Collaborations (%)",
                range=[0, 120],  # Extra space for annotations
                showgrid=True,
                gridcolor='lightgray'
            ),
            yaxis=dict(
                title="Political Factions",
                showgrid=False
            ),
            barmode='stack',
            height=max(400, len(faction_names) * 40),  # Dynamic height based on faction count
            margin=dict(l=150, r=100, t=120, b=50),  # Much more top margin for title and legend
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.05,  # Move legend higher to avoid title overlap
                xanchor="center",
                x=0.5,
                bgcolor="rgba(255,255,255,0.8)",  # Add background to legend for better visibility
                bordercolor="gray",
                borderwidth=1
            ),
            plot_bgcolor='rgba(240,240,240,0.1)'
        )
        
        return fig

    def plot_faction_collaboration_matrix(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        min_collaborations: int = 3,
        show_solo_bills: bool = True,
        min_total_bills: int = 1,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate enhanced faction collaboration matrix showing both collaborations and solo bill activity."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b", **kwargs)

        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(con, ["KNS_Bill", "KNS_BillInitiator", "KNS_PersonToPosition", "KNS_Faction"]):
                    return None

                # Enhanced query to include all active factions and solo bills
                query = f"""
                WITH AllActiveFactions AS (
                    -- Get all factions that initiated bills (main or supporting) in selected Knesset(s)
                    SELECT DISTINCT 
                        f.FactionID, 
                        f.Name as FactionName,
                        COALESCE(ufs.CoalitionStatus, 'Unknown') as CoalitionStatus
                    FROM KNS_Faction f
                    LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID
                    WHERE f.FactionID IN (
                        SELECT DISTINCT 
                            COALESCE(
                                (SELECT f2.FactionID 
                                 FROM KNS_PersonToPosition ptp 
                                 JOIN KNS_Faction f2 ON ptp.FactionID = f2.FactionID
                                 WHERE ptp.PersonID = bi.PersonID 
                                   AND ptp.KnessetNum = b.KnessetNum
                                 ORDER BY ptp.StartDate DESC LIMIT 1),
                                (SELECT f2.FactionID 
                                 FROM KNS_PersonToPosition ptp 
                                 JOIN KNS_Faction f2 ON ptp.FactionID = f2.FactionID
                                 WHERE ptp.PersonID = bi.PersonID
                                 ORDER BY ptp.KnessetNum DESC, ptp.StartDate DESC LIMIT 1)
                            ) as FactionID
                        FROM KNS_BillInitiator bi
                        JOIN KNS_Bill b ON bi.BillID = b.BillID
                        WHERE b.KnessetNum IS NOT NULL 
                          AND {filters["knesset_condition"]}
                          AND bi.PersonID IS NOT NULL
                    )
                    AND f.FactionID IS NOT NULL
                ),
                SoloBills AS (
                    -- Count bills where each faction worked alone (only 1 initiator total)
                    SELECT 
                        af.FactionID,
                        af.FactionName,
                        af.CoalitionStatus,
                        COUNT(DISTINCT solo_bills.BillID) as SoloBillCount
                    FROM AllActiveFactions af
                    LEFT JOIN (
                        SELECT DISTINCT 
                            bi.BillID,
                            COALESCE(
                                (SELECT f.FactionID 
                                 FROM KNS_PersonToPosition ptp 
                                 JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                                 WHERE ptp.PersonID = bi.PersonID 
                                   AND ptp.KnessetNum = b.KnessetNum
                                 ORDER BY ptp.StartDate DESC LIMIT 1),
                                (SELECT f.FactionID 
                                 FROM KNS_PersonToPosition ptp 
                                 JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                                 WHERE ptp.PersonID = bi.PersonID
                                 ORDER BY ptp.KnessetNum DESC, ptp.StartDate DESC LIMIT 1)
                            ) as FactionID
                        FROM KNS_BillInitiator bi
                        JOIN KNS_Bill b ON bi.BillID = b.BillID
                        WHERE bi.BillID IN (
                            SELECT BillID 
                            FROM KNS_BillInitiator 
                            GROUP BY BillID 
                            HAVING COUNT(*) = 1
                        )
                        AND b.KnessetNum IS NOT NULL 
                        AND {filters["knesset_condition"]}
                    ) solo_bills ON af.FactionID = solo_bills.FactionID
                    GROUP BY af.FactionID, af.FactionName, af.CoalitionStatus
                ),
                FactionCollaborations AS (
                    -- Existing collaboration logic
                    SELECT 
                        main.PersonID as MainPersonID,
                        supp.PersonID as SuppPersonID,
                        main.BillID,
                        b.KnessetNum
                    FROM KNS_BillInitiator main
                    JOIN KNS_Bill b ON main.BillID = b.BillID
                    JOIN KNS_BillInitiator supp ON main.BillID = supp.BillID 
                    WHERE main.Ordinal = 1 
                        AND supp.Ordinal > 1
                        AND b.KnessetNum IS NOT NULL
                        AND {filters["knesset_condition"]}
                ),
                CollaborationPairs AS (
                    SELECT 
                        main_faction.FactionID as MainFactionID,
                        supp_faction.FactionID as SupporterFactionID,
                        COUNT(DISTINCT fc.BillID) as CollaborationCount,
                        main_faction.FactionName as MainFactionName,
                        supp_faction.FactionName as SupporterFactionName,
                        main_faction.CoalitionStatus as MainCoalitionStatus,
                        supp_faction.CoalitionStatus as SupporterCoalitionStatus
                    FROM FactionCollaborations fc
                    JOIN (
                        SELECT DISTINCT 
                            fc2.MainPersonID as PersonID,
                            af.FactionID,
                            af.FactionName,
                            af.CoalitionStatus
                        FROM FactionCollaborations fc2
                        JOIN AllActiveFactions af ON af.FactionID = (
                            COALESCE(
                                (SELECT f.FactionID 
                                 FROM KNS_PersonToPosition ptp 
                                 JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                                 WHERE ptp.PersonID = fc2.MainPersonID AND ptp.KnessetNum = fc2.KnessetNum
                                 ORDER BY ptp.StartDate DESC LIMIT 1),
                                (SELECT f.FactionID 
                                 FROM KNS_PersonToPosition ptp 
                                 JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                                 WHERE ptp.PersonID = fc2.MainPersonID
                                 ORDER BY ptp.KnessetNum DESC, ptp.StartDate DESC LIMIT 1)
                            )
                        )
                    ) main_faction ON fc.MainPersonID = main_faction.PersonID
                    JOIN (
                        SELECT DISTINCT 
                            fc2.SuppPersonID as PersonID,
                            af.FactionID,
                            af.FactionName,
                            af.CoalitionStatus
                        FROM FactionCollaborations fc2
                        JOIN AllActiveFactions af ON af.FactionID = (
                            COALESCE(
                                (SELECT f.FactionID 
                                 FROM KNS_PersonToPosition ptp 
                                 JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                                 WHERE ptp.PersonID = fc2.SuppPersonID AND ptp.KnessetNum = fc2.KnessetNum
                                 ORDER BY ptp.StartDate DESC LIMIT 1),
                                (SELECT f.FactionID 
                                 FROM KNS_PersonToPosition ptp 
                                 JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                                 WHERE ptp.PersonID = fc2.SuppPersonID
                                 ORDER BY ptp.KnessetNum DESC, ptp.StartDate DESC LIMIT 1)
                            )
                        )
                    ) supp_faction ON fc.SuppPersonID = supp_faction.PersonID
                    WHERE main_faction.FactionID IS NOT NULL
                        AND supp_faction.FactionID IS NOT NULL
                        AND main_faction.FactionID <> supp_faction.FactionID
                    GROUP BY main_faction.FactionID, supp_faction.FactionID, 
                             main_faction.FactionName, supp_faction.FactionName,
                             main_faction.CoalitionStatus, supp_faction.CoalitionStatus
                    HAVING COUNT(DISTINCT fc.BillID) >= {min_collaborations}
                )
                -- Return results in format compatible with existing matrix creation
                SELECT 
                    'collaboration' as DataType,
                    cp.MainFactionID as FactionID1,
                    cp.SupporterFactionID as FactionID2,
                    cp.MainFactionName as FactionName1,
                    cp.SupporterFactionName as FactionName2,
                    cp.MainCoalitionStatus as CoalitionStatus1,
                    cp.SupporterCoalitionStatus as CoalitionStatus2,
                    cp.CollaborationCount as Count
                FROM CollaborationPairs cp
                UNION ALL
                SELECT 
                    'solo' as DataType,
                    sb.FactionID as FactionID1,
                    sb.FactionID as FactionID2,
                    sb.FactionName as FactionName1,
                    sb.FactionName as FactionName2,
                    sb.CoalitionStatus as CoalitionStatus1,
                    sb.CoalitionStatus as CoalitionStatus2,
                    sb.SoloBillCount as Count
                FROM SoloBills sb
                WHERE ({show_solo_bills} = 1 AND sb.SoloBillCount >= {min_total_bills})
                   OR (sb.FactionID IN (SELECT DISTINCT MainFactionID FROM CollaborationPairs))
                   OR (sb.FactionID IN (SELECT DISTINCT SupporterFactionID FROM CollaborationPairs))
                ORDER BY Count DESC
                """

                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(f"No faction activity data found for '{filters['knesset_title']}'.")
                    return None

                # Create enhanced matrix visualization
                return self._create_enhanced_faction_matrix_chart(
                    df, filters['knesset_title'], min_collaborations, show_solo_bills
                )

        except Exception as e:
            self.logger.error(f"Error generating faction collaboration matrix: {e}", exc_info=True)
            st.error(f"Could not generate faction collaboration matrix: {e}")
            return None

    def plot_faction_collaboration_chord(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        min_collaborations: int = 5,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate faction collaboration chord diagram showing circular collaboration flows."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b", **kwargs)

        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(con, ["KNS_Bill", "KNS_BillInitiator", "KNS_PersonToPosition", "KNS_Faction"]):
                    return None

                # Use similar query as matrix but aggregate bidirectionally for chord diagram
                query = f"""
                WITH FactionCollaborations AS (
                    SELECT 
                        main.PersonID as MainPersonID,
                        supp.PersonID as SuppPersonID,
                        main.BillID,
                        b.KnessetNum
                    FROM KNS_BillInitiator main
                    JOIN KNS_Bill b ON main.BillID = b.BillID
                    JOIN KNS_BillInitiator supp ON main.BillID = supp.BillID 
                    WHERE main.Ordinal = 1 
                        AND supp.Ordinal > 1
                        AND b.KnessetNum IS NOT NULL
                        AND {filters["knesset_condition"]}
                ),
                PersonFactions AS (
                    SELECT DISTINCT
                        fc.MainPersonID as PersonID,
                        COALESCE(
                            (SELECT f.FactionID 
                             FROM KNS_PersonToPosition ptp 
                             JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                             WHERE ptp.PersonID = fc.MainPersonID AND ptp.KnessetNum = fc.KnessetNum
                             ORDER BY ptp.StartDate DESC LIMIT 1),
                            (SELECT f.FactionID 
                             FROM KNS_PersonToPosition ptp 
                             JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                             WHERE ptp.PersonID = fc.MainPersonID
                             ORDER BY ptp.KnessetNum DESC, ptp.StartDate DESC LIMIT 1)
                        ) as FactionID
                    FROM FactionCollaborations fc
                    UNION
                    SELECT DISTINCT
                        fc.SuppPersonID as PersonID,
                        COALESCE(
                            (SELECT f.FactionID 
                             FROM KNS_PersonToPosition ptp 
                             JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                             WHERE ptp.PersonID = fc.SuppPersonID AND ptp.KnessetNum = fc.KnessetNum
                             ORDER BY ptp.StartDate DESC LIMIT 1),
                            (SELECT f.FactionID 
                             FROM KNS_PersonToPosition ptp 
                             JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
                             WHERE ptp.PersonID = fc.SuppPersonID
                             ORDER BY ptp.KnessetNum DESC, ptp.StartDate DESC LIMIT 1)
                        ) as FactionID
                    FROM FactionCollaborations fc
                ),
                FactionStats AS (
                    SELECT 
                        f.FactionID,
                        f.Name as FactionName,
                        COALESCE(ufs.CoalitionStatus, 'Unknown') as CoalitionStatus,
                        COUNT(DISTINCT fc.BillID) as TotalCollaborations
                    FROM KNS_Faction f
                    LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID
                    JOIN PersonFactions pf ON f.FactionID = pf.FactionID
                    JOIN FactionCollaborations fc ON pf.PersonID IN (fc.MainPersonID, fc.SuppPersonID)
                    GROUP BY f.FactionID, f.Name, ufs.CoalitionStatus
                    HAVING COUNT(DISTINCT fc.BillID) >= {min_collaborations}
                )
                SELECT 
                    fs.FactionID,
                    fs.FactionName,
                    fs.CoalitionStatus,
                    fs.TotalCollaborations
                FROM FactionStats fs
                ORDER BY fs.TotalCollaborations DESC
                """

                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(f"No faction collaboration chord data found for '{filters['knesset_title']}' with minimum {min_collaborations} collaborations.")
                    return None

                # Create chord diagram visualization 
                return self._create_faction_chord_chart(df, filters['knesset_title'])

        except Exception as e:
            self.logger.error(f"Error generating faction collaboration chord: {e}", exc_info=True)
            st.error(f"Could not generate faction collaboration chord: {e}")
            return None

    def _create_faction_matrix_chart(self, df: pd.DataFrame, title_suffix: str, min_collaborations: int) -> go.Figure:
        """Create faction collaboration matrix heatmap chart."""
        
        # Create pivot table for matrix structure
        pivot_data = df.pivot_table(
            index='MainFactionName', 
            columns='SupporterFactionName', 
            values='CollaborationCount',
            fill_value=0
        )
        
        # Get faction coalition status for color coding
        faction_status = {}
        for _, row in df.iterrows():
            faction_status[row['MainFactionName']] = row['MainCoalitionStatus']
            faction_status[row['SupporterFactionName']] = row['SupporterCoalitionStatus']
        
        # Sort factions by coalition status and total collaborations
        faction_totals = {}
        for faction in pivot_data.index:
            faction_totals[faction] = pivot_data.loc[faction].sum() + pivot_data[faction].sum()
        
        # Sort by coalition status then by total collaborations
        def sort_key(faction):
            status = faction_status.get(faction, 'Unknown')
            total = faction_totals.get(faction, 0)
            # Coalition first, then Opposition, then Unknown; within each group, sort by total descending
            status_order = {'Coalition': 0, 'Opposition': 1, 'Unknown': 2}
            return (status_order.get(status, 3), -total)
        
        sorted_factions = sorted(pivot_data.index, key=sort_key)
        pivot_data = pivot_data.reindex(index=sorted_factions, columns=sorted_factions)
        
        # Create hover text with detailed information
        hover_text = []
        for i, main_faction in enumerate(pivot_data.index):
            hover_row = []
            for j, supp_faction in enumerate(pivot_data.columns):
                if i == j:  # Diagonal - same faction
                    hover_row.append(f"<b>Same Faction</b><br>{main_faction}")
                else:
                    collab_count = pivot_data.iloc[i, j]
                    main_status = faction_status.get(main_faction, 'Unknown')
                    supp_status = faction_status.get(supp_faction, 'Unknown')
                    
                    if collab_count > 0:
                        hover_row.append(
                            f"<b>{main_faction}</b> → <b>{supp_faction}</b><br>"
                            f"Collaborations: {collab_count}<br>"
                            f"Main Status: {main_status}<br>"
                            f"Supporter Status: {supp_status}"
                        )
                    else:
                        hover_row.append(
                            f"<b>{main_faction}</b> → <b>{supp_faction}</b><br>"
                            f"No collaborations (< {min_collaborations})<br>"
                            f"Main Status: {main_status}<br>"
                            f"Supporter Status: {supp_status}"
                        )
            hover_text.append(hover_row)
        
        # Create color scale - blue for low, red for high
        max_collab = pivot_data.max().max()
        colorscale = [
            [0, 'white'],
            [0.1, '#e6f3ff'],
            [0.3, '#b3d9ff'],
            [0.6, '#66c2ff'],
            [0.8, '#1a8cff'],
            [1.0, '#0066cc']
        ]
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=pivot_data.values,
            x=pivot_data.columns,
            y=pivot_data.index,
            colorscale=colorscale,
            hovertemplate='%{customdata}<extra></extra>',
            customdata=hover_text,
            colorbar=dict(
                title="Collaboration Count",
                titleside="right",
                thickness=15,
                len=0.8
            ),
            showscale=True
        ))
        
        # Add annotations for coalition status on axes
        fig.update_layout(
            title=f"<b>📊 Faction Collaboration Matrix<br>{title_suffix}</b>",
            title_x=0.5,
            xaxis=dict(
                title="Supporting Factions",
                side='bottom',
                tickangle=45
            ),
            yaxis=dict(
                title="Primary Initiating Factions",
                tickmode='linear'
            ),
            height=max(600, len(pivot_data) * 30),
            width=max(800, len(pivot_data) * 30),
            margin=dict(l=200, r=100, t=100, b=150),
            plot_bgcolor='white'
        )
        
        # Add coalition status color coding to axis labels
        fig.update_xaxes(tickfont=dict(color='black', size=10))
        fig.update_yaxes(tickfont=dict(color='black', size=10))
        
        return fig

    def _create_enhanced_faction_matrix_chart(self, df: pd.DataFrame, title_suffix: str, 
                                            min_collaborations: int, show_solo_bills: bool) -> go.Figure:
        """Create enhanced faction collaboration matrix with both collaborations and solo bills."""
        
        # Separate solo and collaboration data
        solo_data = df[df['DataType'] == 'solo'].copy()
        collab_data = df[df['DataType'] == 'collaboration'].copy()
        
        # Get all unique factions
        all_factions = set()
        
        for _, row in df.iterrows():
            all_factions.add(row['FactionName1'])
            if row['FactionName1'] != row['FactionName2']:  # Don't add same faction twice for solo bills
                all_factions.add(row['FactionName2'])
        
        all_factions = sorted(list(all_factions))
        
        # Get faction coalition status mapping
        faction_status = {}
        for _, row in df.iterrows():
            faction_status[row['FactionName1']] = row['CoalitionStatus1']
            if row['FactionName1'] != row['FactionName2']:
                faction_status[row['FactionName2']] = row['CoalitionStatus2']
        
        # Sort factions by coalition status and activity level
        def sort_key(faction):
            status = faction_status.get(faction, 'Unknown')
            # Calculate total activity for this faction
            total_activity = 0
            
            # Add solo bills
            solo_count = solo_data[solo_data['FactionName1'] == faction]['Count'].sum()
            total_activity += solo_count
            
            # Add collaboration activity
            collab_as_main = collab_data[collab_data['FactionName1'] == faction]['Count'].sum()
            collab_as_supporter = collab_data[collab_data['FactionName2'] == faction]['Count'].sum()
            total_activity += collab_as_main + collab_as_supporter
            
            # Coalition first, then Opposition, then Unknown; within each group, sort by total descending
            status_order = {'Coalition': 0, 'Opposition': 1, 'Unknown': 2}
            return (status_order.get(status, 3), -total_activity)
        
        sorted_factions = sorted(all_factions, key=sort_key)
        n_factions = len(sorted_factions)
        
        # Create full matrix
        matrix_data = np.zeros((n_factions, n_factions))
        matrix_type = np.full((n_factions, n_factions), 'none', dtype=object)  # Track data type
        
        # Fill diagonal with solo bills
        if show_solo_bills:
            for _, row in solo_data.iterrows():
                if row['FactionName1'] in sorted_factions:
                    idx = sorted_factions.index(row['FactionName1'])
                    matrix_data[idx, idx] = row['Count']
                    matrix_type[idx, idx] = 'solo'
        
        # Fill off-diagonal with collaborations
        for _, row in collab_data.iterrows():
            if row['FactionName1'] in sorted_factions and row['FactionName2'] in sorted_factions:
                idx1 = sorted_factions.index(row['FactionName1'])
                idx2 = sorted_factions.index(row['FactionName2'])
                matrix_data[idx1, idx2] = row['Count']
                matrix_type[idx1, idx2] = 'collaboration'
        
        # Create custom hover text
        hover_text = []
        for i in range(n_factions):
            hover_row = []
            for j in range(n_factions):
                faction1 = sorted_factions[i]
                faction2 = sorted_factions[j]
                value = matrix_data[i, j]
                data_type = matrix_type[i, j]
                
                if i == j:  # Diagonal - solo bills
                    if show_solo_bills and value > 0:
                        hover_row.append(
                            f"<b>{faction1}</b><br>"
                            f"Solo Bills: {int(value)}<br>"
                            f"Status: {faction_status.get(faction1, 'Unknown')}<br>"
                            f"<i>Bills with only 1 initiator</i>"
                        )
                    else:
                        hover_row.append(
                            f"<b>{faction1}</b><br>"
                            f"Solo Bills: 0<br>"
                            f"Status: {faction_status.get(faction1, 'Unknown')}"
                        )
                else:  # Off-diagonal - collaborations
                    if value > 0:
                        hover_row.append(
                            f"<b>{faction1}</b> → <b>{faction2}</b><br>"
                            f"Collaborations: {int(value)}<br>"
                            f"Primary: {faction_status.get(faction1, 'Unknown')}<br>"
                            f"Supporter: {faction_status.get(faction2, 'Unknown')}<br>"
                            f"<i>Bills with cross-party support</i>"
                        )
                    else:
                        hover_row.append(
                            f"<b>{faction1}</b> → <b>{faction2}</b><br>"
                            f"No collaboration (< {min_collaborations})<br>"
                            f"Primary: {faction_status.get(faction1, 'Unknown')}<br>"
                            f"Supporter: {faction_status.get(faction2, 'Unknown')}"
                        )
            hover_text.append(hover_row)
        
        # Create dual-color visualization using custom colorscale
        # We'll create two separate traces: one for solo bills (diagonal) and one for collaborations
        
        # First, create collaboration matrix (set diagonal to 0)
        collab_matrix = matrix_data.copy()
        np.fill_diagonal(collab_matrix, 0)
        
        # Solo matrix (only diagonal)
        solo_matrix = np.zeros_like(matrix_data)
        np.fill_diagonal(solo_matrix, np.diag(matrix_data))
        
        fig = go.Figure()
        
        # Add collaboration heatmap (off-diagonal)
        if collab_matrix.max() > 0:
            fig.add_trace(go.Heatmap(
                z=collab_matrix,
                x=sorted_factions,
                y=sorted_factions,
                colorscale=[
                    [0, 'rgba(255,255,255,0)'],  # Transparent for zero values
                    [0.001, 'white'],
                    [0.1, '#e6f3ff'],
                    [0.3, '#b3d9ff'], 
                    [0.6, '#66c2ff'],
                    [0.8, '#1a8cff'],
                    [1.0, '#0066cc']
                ],
                name="Collaborations",
                hovertemplate='%{customdata}<extra></extra>',
                customdata=hover_text,
                colorbar=dict(
                    title="Collaboration Count",
                    titleside="right",
                    thickness=15,
                    len=0.8,
                    x=1.02,
                    xanchor="left"
                ),
                zmin=0,
                zmax=collab_matrix.max() if collab_matrix.max() > 0 else 1,
                showscale=True
            ))
        
        # Add solo bills heatmap (diagonal only)
        if show_solo_bills and solo_matrix.max() > 0:
            fig.add_trace(go.Heatmap(
                z=solo_matrix,
                x=sorted_factions,
                y=sorted_factions,
                colorscale=[
                    [0, 'rgba(255,255,255,0)'],  # Transparent for zero values
                    [0.001, 'white'],
                    [0.1, '#e6ffe6'],
                    [0.3, '#b3ffb3'],
                    [0.6, '#66ff66'], 
                    [0.8, '#1aff1a'],
                    [1.0, '#00cc00']
                ],
                name="Solo Bills",
                hovertemplate='%{customdata}<extra></extra>',
                customdata=hover_text,
                colorbar=dict(
                    title="Solo Bills Count",
                    titleside="right", 
                    thickness=15,
                    len=0.8,
                    x=1.12,
                    xanchor="left"
                ),
                zmin=0,
                zmax=solo_matrix.max() if solo_matrix.max() > 0 else 1,
                showscale=True
            ))
        
        # Update layout
        title_text = f"<b>📊 Enhanced Faction Collaboration Matrix<br>{title_suffix}</b>"
        if show_solo_bills:
            title_text += "<br><sub>Blue: Inter-faction collaborations | Green: Solo bills (diagonal)</sub>"
        
        fig.update_layout(
            title=title_text,
            title_x=0.5,
            xaxis=dict(
                title="Supporting/Target Factions",
                side='bottom',
                tickangle=45,
                tickfont=dict(size=10)
            ),
            yaxis=dict(
                title="Primary Initiating Factions",
                tickmode='linear',
                tickfont=dict(size=10),
                autorange='reversed'  # Show first faction at top
            ),
            height=max(700, n_factions * 35),
            width=max(900, n_factions * 35), 
            margin=dict(l=250, r=280, t=120, b=180),
            plot_bgcolor='white'
        )
        
        return fig

    def _create_faction_chord_chart(self, df: pd.DataFrame, title_suffix: str) -> go.Figure:
        """Create faction collaboration chord diagram."""
        
        # For a simplified chord-like visualization using plotly, we'll create a circular scatter plot
        # with faction points and curved connections showing collaboration strength
        
        n_factions = len(df)
        if n_factions == 0:
            fig = go.Figure()
            fig.add_annotation(text="No faction data available", 
                             xref="paper", yref="paper", x=0.5, y=0.5,
                             showarrow=False, font=dict(size=16))
            return fig
        
        # Arrange factions in a circle
        angles = np.linspace(0, 2*np.pi, n_factions, endpoint=False)
        radius = 100
        
        # Calculate positions
        faction_positions = {}
        for i, (_, faction) in enumerate(df.iterrows()):
            x = radius * np.cos(angles[i])
            y = radius * np.sin(angles[i])
            faction_positions[faction['FactionName']] = (x, y, angles[i])
        
        # Create figure
        fig = go.Figure()
        
        # Color mapping for coalition status
        status_colors = {
            'Coalition': '#1f77b4',
            'Opposition': '#ff7f0e',
            'Unknown': '#808080'
        }
        
        # Add faction nodes grouped by status
        max_collaborations = df['TotalCollaborations'].max()
        
        for status in ['Coalition', 'Opposition', 'Unknown']:
            status_factions = df[df['CoalitionStatus'] == status]
            if status_factions.empty:
                continue
                
            faction_x = []
            faction_y = []
            faction_sizes = []
            faction_names = []
            hover_texts = []
            
            for _, faction in status_factions.iterrows():
                pos = faction_positions[faction['FactionName']]
                faction_x.append(pos[0])
                faction_y.append(pos[1])
                
                # Size based on total collaborations
                size = max(30, min(80, 30 + (faction['TotalCollaborations'] / max_collaborations * 50)))
                faction_sizes.append(size)
                faction_names.append(faction['FactionName'])
                
                hover_text = (
                    f"<b>{faction['FactionName']}</b><br>"
                    f"Status: {faction['CoalitionStatus']}<br>"
                    f"Total Collaborations: {faction['TotalCollaborations']}"
                )
                hover_texts.append(hover_text)
            
            # Add faction trace
            fig.add_trace(go.Scatter(
                x=faction_x,
                y=faction_y,
                mode='markers+text',
                marker=dict(
                    size=faction_sizes,
                    color=status_colors.get(status, '#808080'),
                    line=dict(width=3, color='white'),
                    opacity=0.9
                ),
                text=faction_names,
                textposition="middle center",
                textfont=dict(size=10, color='white', family="Arial Black"),
                hovertext=hover_texts,
                hoverinfo='text',
                name=status,
                showlegend=True
            ))
        
        # Add curved connections between factions (simplified approach)
        # Note: Full chord diagram implementation would require more complex path calculations
        
        fig.update_layout(
            title=f"<b>🔄 Faction Collaboration Chord Diagram<br>{title_suffix}</b>",
            title_x=0.5,
            showlegend=True,
            hovermode='closest',
            margin=dict(b=40, l=40, r=40, t=80),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-150, 150]),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[-150, 150]),
            height=800,
            width=800,
            plot_bgcolor='rgba(240,240,240,0.1)',
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.05,
                font=dict(size=12)
            ),
            annotations=[
                dict(
                    text="Node size represents total collaborations<br>Colors indicate coalition status",
                    showarrow=False,
                    xref="paper", yref="paper",
                    x=0.02, y=0.02,
                    xanchor='left', yanchor='bottom',
                    font=dict(size=10),
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="gray",
                    borderwidth=1
                )
            ]
        )
        
        return fig

    def generate(self, chart_type: str, **kwargs) -> Optional[go.Figure]:
        """Generate the requested network chart."""
        chart_methods = {
            "mk_collaboration_network": self.plot_mk_collaboration_network,
            "faction_collaboration_network": self.plot_faction_collaboration_network,
            "faction_collaboration_matrix": self.plot_faction_collaboration_matrix,
            "faction_collaboration_chord": self.plot_faction_collaboration_chord,
            "faction_coalition_breakdown": self.plot_faction_coalition_breakdown,
        }

        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown network chart type: {chart_type}")
            return None