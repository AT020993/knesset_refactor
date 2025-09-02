# Faction Collaboration Chart Improvements

## Overview
Enhanced the faction collaboration visualization with new chart types to address interpretability issues with the original network chart.

## Problem Addressed
The original `faction_collaboration_network` chart was hard to interpret due to:
- Complex force-directed network layout with overlapping connections
- Difficulty comparing collaboration strengths between faction pairs
- Poor visibility of which factions collaborate most with each other
- Coalition vs Opposition patterns were unclear

## New Chart Types Implemented

### 1. **Faction Collaboration Matrix** (`faction_collaboration_matrix`)
**Primary Recommendation** - Interactive heatmap matrix showing collaboration counts between all faction pairs.

**Features:**
- Factions on both X and Y axes 
- Cell intensity shows collaboration count (white = none, blue gradient = more)
- Sortable by coalition status (Coalition → Opposition → Unknown)
- Within each group, sorted by total collaboration count
- Detailed hover information showing collaboration details
- Easy to scan and compare relationships

**Use Case:** Quick identification of which factions collaborate most with whom

### 2. **Faction Collaboration Chord Diagram** (`faction_collaboration_chord`)
Circular layout with faction segments sized by total activity.

**Features:**
- Factions arranged in circle
- Node size reflects total collaborations
- Color coding by coalition status (Blue=Coalition, Orange=Opposition, Gray=Unknown)
- Interactive hover with collaboration details
- Simplified chord-style visualization using Plotly

**Use Case:** Visually appealing overview of collaboration patterns

### 3. **Enhanced Original Network** (kept for backward compatibility)
The original network chart remains available for users who prefer it.

## Implementation Details

### Files Modified
1. **`src/ui/charts/network.py`**:
   - Added `plot_faction_collaboration_matrix()` method
   - Added `plot_faction_collaboration_chord()` method  
   - Added `_create_faction_matrix_chart()` helper
   - Added `_create_faction_chord_chart()` helper
   - Updated `generate()` method to include new chart types

2. **`src/ui/charts/factory.py`**:
   - Added new chart types to `get_available_charts()` network section

3. **`src/ui/services/chart_service.py`**:
   - Added `plot_faction_collaboration_matrix()` service method
   - Added `plot_faction_collaboration_chord()` service method

4. **`src/ui/plot_generators.py`**:
   - Added legacy wrapper functions for UI compatibility
   - Updated `get_available_plots()` to include new chart options

### Database Queries
Both new charts reuse the existing collaboration detection logic:
- **Primary Initiators**: `main.Ordinal = 1` (bill sponsors)
- **Supporting Initiators**: `supp.Ordinal > 1` (co-sponsors) 
- **Collaboration Detection**: JOIN on same BillID between primary and supporting initiators
- **Advanced Faction Mapping**: Multi-level COALESCE queries with fallback logic for 100% faction coverage
- **Minimum Collaboration Threshold**: Configurable (default 3+ bills for matrix, 5+ for chord)

### UI Integration
New charts are available in the "Collaboration Networks" section:
- **MK Collaboration Network** (existing)
- **Faction Collaboration Network** (existing) 
- **Faction Collaboration Matrix** ⭐ **NEW - Recommended**
- **Faction Collaboration Chord** ⭐ **NEW**
- **Faction Coalition Breakdown** (existing)

## Benefits Achieved

### ✅ **Clear Comparisons**
Matrix layout makes it easy to scan which factions collaborate most with whom

### ✅ **Better UX** 
Intuitive heatmap interface vs confusing network connections

### ✅ **Retained Options**
Original network chart preserved for users who prefer it

### ✅ **Enhanced Insights**
Multiple perspectives on the same collaboration data:
- **Matrix**: Detailed pairwise comparisons
- **Chord**: Visual overview with size relationships
- **Network**: Connection topology (original)
- **Breakdown**: Coalition vs Opposition percentages

### ✅ **Coalition Analysis**
Clear visual separation and analysis of cross-party vs same-party collaboration patterns

## Usage Recommendation

**For most users**: Start with **Faction Collaboration Matrix** as it provides the clearest view of "which faction collaborates most with which other factions."

**For presentations**: Use **Faction Collaboration Chord** for a visually appealing overview.

**For network analysis**: Continue using the original **Faction Collaboration Network** if topology relationships are important.

## Technical Validation
- ✅ All syntax validation passed
- ✅ Proper integration with existing chart factory system
- ✅ Legacy compatibility maintained 
- ✅ Clean architecture principles followed
- ✅ Error handling and logging implemented