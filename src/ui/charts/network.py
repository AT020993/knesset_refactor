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

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b")

        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(con, ["KNS_Bill", "KNS_BillInitiator", "KNS_Person", "KNS_PersonToPosition", "KNS_Faction"]):
                    return None

                # Get MK collaboration network data
                query = f"""
                WITH BillCollaborations AS (
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
                        COALESCE(f.Name, 'Unknown') as FactionName,
                        COUNT(DISTINCT bi.BillID) as TotalBills
                    FROM KNS_Person p
                    LEFT JOIN KNS_BillInitiator bi ON p.PersonID = bi.PersonID AND bi.Ordinal = 1
                    LEFT JOIN KNS_Bill b ON bi.BillID = b.BillID
                    LEFT JOIN KNS_PersonToPosition ptp ON p.PersonID = ptp.PersonID AND b.KnessetNum = ptp.KnessetNum
                    LEFT JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
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

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b")

        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(con, ["KNS_Bill", "KNS_BillInitiator", "KNS_PersonToPosition", "KNS_Faction"]):
                    return None

                # Get faction collaboration network data - simplified query
                query = f"""
                SELECT 
                    main_f.FactionID as MainFactionID,
                    supp_f.FactionID as SupporterFactionID,
                    COUNT(DISTINCT main.BillID) as CollaborationCount,
                    main_f.Name as MainFactionName,
                    supp_f.Name as SupporterFactionName,
                    'Unknown' as MainCoalitionStatus,
                    'Unknown' as SupporterCoalitionStatus,
                    0 as MainFactionTotalBills,
                    0 as SupporterFactionTotalBills
                FROM KNS_BillInitiator main
                JOIN KNS_Bill b ON main.BillID = b.BillID
                JOIN KNS_BillInitiator supp ON main.BillID = supp.BillID 
                LEFT JOIN KNS_PersonToPosition main_ptp ON main.PersonID = main_ptp.PersonID AND b.KnessetNum = main_ptp.KnessetNum
                LEFT JOIN KNS_PersonToPosition supp_ptp ON supp.PersonID = supp_ptp.PersonID AND b.KnessetNum = supp_ptp.KnessetNum
                LEFT JOIN KNS_Faction main_f ON main_ptp.FactionID = main_f.FactionID
                LEFT JOIN KNS_Faction supp_f ON supp_ptp.FactionID = supp_f.FactionID
                WHERE main.Ordinal = 1 
                    AND supp.Ordinal > 1
                    AND main_f.FactionID IS NOT NULL
                    AND supp_f.FactionID IS NOT NULL
                    AND main_f.FactionID <> supp_f.FactionID
                    AND b.KnessetNum IS NOT NULL
                    AND {filters["knesset_condition"]}
                GROUP BY main_f.FactionID, supp_f.FactionID, main_f.Name, supp_f.Name
                HAVING COUNT(DISTINCT main.BillID) >= {min_collaborations}
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

    def plot_coalition_opposition_network(
        self,
        knesset_filter: Optional[List[int]] = None,
        faction_filter: Optional[List[str]] = None,
        min_collaborations: int = 3,
        **kwargs,
    ) -> Optional[go.Figure]:
        """Generate coalition/opposition collaboration network chart showing cross-party cooperation."""
        if not self.check_database_exists():
            return None

        filters = self.build_filters(knesset_filter, faction_filter, table_prefix="b")

        try:
            with get_db_connection(
                self.db_path, read_only=False, logger_obj=self.logger
            ) as con:
                if not self.check_tables_exist(con, ["KNS_Bill", "KNS_BillInitiator", "KNS_PersonToPosition", "KNS_Faction", "UserFactionCoalitionStatus"]):
                    return None

                # Get coalition/opposition collaboration data - simplified query
                query = f"""
                SELECT 
                    COALESCE(main_ufs.CoalitionStatus, 'Unknown') as MainCoalitionStatus,
                    COALESCE(supp_ufs.CoalitionStatus, 'Unknown') as SupporterCoalitionStatus,
                    main_f.Name as MainFactionName,
                    supp_f.Name as SupporterFactionName,
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
                    AND main_ufs.CoalitionStatus IS NOT NULL
                    AND supp_ufs.CoalitionStatus IS NOT NULL
                    AND main_ufs.CoalitionStatus <> supp_ufs.CoalitionStatus
                    AND b.KnessetNum IS NOT NULL
                    AND {filters["knesset_condition"]}
                GROUP BY main_ufs.CoalitionStatus, supp_ufs.CoalitionStatus, main_f.Name, supp_f.Name
                HAVING COUNT(DISTINCT main.BillID) >= {min_collaborations}
                ORDER BY CollaborationCount DESC
                """

                df = safe_execute_query(con, query, self.logger)

                if df.empty:
                    st.info(f"No cross-party collaboration data found for '{filters['knesset_title']}' with minimum {min_collaborations} collaborations.")
                    return None

                # Create network visualization for coalition/opposition
                return self._create_coalition_network_chart(df, filters['knesset_title'])

        except Exception as e:
            self.logger.error(f"Error generating coalition/opposition collaboration network: {e}", exc_info=True)
            st.error(f"Could not generate coalition/opposition collaboration network: {e}")
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

    def _create_mk_network_chart(self, df: pd.DataFrame, title_suffix: str) -> go.Figure:
        """Create MK collaboration network chart."""
        
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
        
        # Generate circular layout
        n_nodes = len(all_nodes)
        angles = np.linspace(0, 2*np.pi, n_nodes, endpoint=False)
        radius = max(10, n_nodes / 3)
        
        node_positions = {}
        for i, (_, node) in enumerate(all_nodes.iterrows()):
            node_id = node['PersonID']
            x = radius * np.cos(angles[i])
            y = radius * np.sin(angles[i])
            node_positions[node_id] = (x, y)
        
        fig = go.Figure()
        
        # Add edges
        edge_x, edge_y = [], []
        for _, edge in df.iterrows():
            source_pos = node_positions[edge['MainInitiatorID']]
            target_pos = node_positions[edge['SupporterID']]
            edge_x.extend([source_pos[0], target_pos[0], None])
            edge_y.extend([source_pos[1], target_pos[1], None])
        
        fig.add_trace(go.Scatter(
            x=edge_x, y=edge_y,
            mode='lines',
            line=dict(width=1, color='#888'),
            hoverinfo='none',
            showlegend=False
        ))
        
        # Add nodes by faction
        unique_factions = all_nodes['Faction'].unique()
        colors = px.colors.qualitative.Set3[:len(unique_factions)]
        color_map = dict(zip(unique_factions, colors))
        
        for faction in unique_factions:
            try:
                faction_nodes = all_nodes[all_nodes['Faction'] == faction]
                
                node_x = [node_positions[node_id][0] for node_id in faction_nodes['PersonID']]
                node_y = [node_positions[node_id][1] for node_id in faction_nodes['PersonID']]
                
                max_bills = all_nodes['TotalBills'].max() if not all_nodes['TotalBills'].empty else 1
                node_sizes = [max(8, min(30, 8 + (bills / max_bills * 20))) for bills in faction_nodes['TotalBills']]
                
                hover_text = [f"{name}<br>{faction}<br>Total Bills: {bills}" 
                             for name, bills in zip(faction_nodes['Name'], faction_nodes['TotalBills'])]
                
                # Ensure faction name is properly encoded for display
                faction_display = str(faction) if faction else 'Unknown'
                
                fig.add_trace(go.Scatter(
                    x=node_x, y=node_y,
                    mode='markers+text',
                    marker=dict(size=node_sizes, color=color_map[faction], line=dict(width=2, color='white')),
                    text=faction_nodes['Name'].tolist(),
                    textposition="middle center",
                    textfont=dict(size=8),
                    hovertext=hover_text,
                    hoverinfo='text',
                    name=faction_display,
                    showlegend=True
                ))
            except Exception as e:
                self.logger.error(f"Error processing faction '{faction}': {e}")
                continue
        
        fig.update_layout(
            title=f"<b>MK Collaboration Network<br>{title_suffix}</b>",
            title_x=0.5,
            showlegend=True,
            hovermode='closest',
            margin=dict(b=20,l=5,r=5,t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            height=700
        )
        
        return fig

    def _create_faction_network_chart(self, df: pd.DataFrame, title_suffix: str) -> go.Figure:
        """Create faction collaboration network chart."""
        
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
            
        except Exception as e:
            self.logger.error(f"Error processing faction data: {e}")
            # Return empty chart on error
            fig = go.Figure()
            fig.add_annotation(text="Error processing faction network data", 
                             xref="paper", yref="paper", x=0.5, y=0.5,
                             showarrow=False, font=dict(size=16))
            return fig
        
        # Generate circular layout
        n_nodes = len(all_factions)
        angles = np.linspace(0, 2*np.pi, n_nodes, endpoint=False)
        radius = max(10, n_nodes / 3)
        
        node_positions = {}
        for i, (_, faction) in enumerate(all_factions.iterrows()):
            faction_id = faction['FactionID']
            x = radius * np.cos(angles[i])
            y = radius * np.sin(angles[i])
            node_positions[faction_id] = (x, y)
        
        fig = go.Figure()
        
        # Add edges
        edge_x, edge_y = [], []
        for _, edge in df.iterrows():
            source_pos = node_positions[edge['MainFactionID']]
            target_pos = node_positions[edge['SupporterFactionID']]
            edge_x.extend([source_pos[0], target_pos[0], None])
            edge_y.extend([source_pos[1], target_pos[1], None])
        
        fig.add_trace(go.Scatter(
            x=edge_x, y=edge_y,
            mode='lines',
            line=dict(width=1, color='#888'),
            hoverinfo='none',
            showlegend=False
        ))
        
        # Add faction nodes
        try:
            node_x = [node_positions[faction_id][0] for faction_id in all_factions['FactionID']]
            node_y = [node_positions[faction_id][1] for faction_id in all_factions['FactionID']]
            
            hover_text = [f"{str(name)}<br>Status: {str(status)}" for name, status in zip(all_factions['Name'], all_factions['Status'])]
            
            fig.add_trace(go.Scatter(
                x=node_x, y=node_y,
                mode='markers+text',
                marker=dict(size=20, color='lightblue', line=dict(width=2, color='white')),
                text=all_factions['Name'].astype(str).tolist(),
                textposition="middle center",
                textfont=dict(size=8),
                hovertext=hover_text,
                hoverinfo='text',
                name='Factions',
                showlegend=False
            ))
        except Exception as e:
            self.logger.error(f"Error creating faction nodes: {e}")
            # Create a simple fallback visualization
            fig.add_trace(go.Scatter(
                x=[0], y=[0],
                mode='markers+text',
                marker=dict(size=20, color='lightblue'),
                text=['Network Error'],
                name='Error',
                showlegend=False
            ))
        
        fig.update_layout(
            title=f"<b>Faction Collaboration Network<br>{title_suffix}</b>",
            title_x=0.5,
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20,l=5,r=5,t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            height=700
        )
        
        return fig

    def generate(self, chart_type: str, **kwargs) -> Optional[go.Figure]:
        """Generate the requested network chart."""
        chart_methods = {
            "mk_collaboration_network": self.plot_mk_collaboration_network,
            "faction_collaboration_network": self.plot_faction_collaboration_network,
            "coalition_opposition_network": self.plot_coalition_opposition_network,
        }

        method = chart_methods.get(chart_type)
        if method:
            return method(**kwargs)
        else:
            self.logger.warning(f"Unknown network chart type: {chart_type}")
            return None