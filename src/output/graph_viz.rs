//! Graph Visualization
//!
//! ASCII and other graph visualization formats.

#![allow(dead_code)]

use anyhow::Result;
use colored::Colorize;
use std::collections::{HashMap, HashSet};

use crate::storage::{GraphView, Node};

/// Graph renderer
pub struct GraphRenderer {
    max_depth: u32,
    max_nodes: usize,
}

impl GraphRenderer {
    /// Create a new graph renderer
    pub fn new() -> Self {
        Self {
            max_depth: 5,
            max_nodes: 100,
        }
    }

    /// Render graph as ASCII art
    pub fn render_ascii(&self, graph: &GraphView) -> Result<()> {
        if graph.nodes.is_empty() {
            println!("{}", "(empty graph)".bright_black());
            return Ok(());
        }

        println!();
        println!("{}", "Graph View".bright_yellow().bold());
        println!("{}", "═".repeat(60));

        // Build adjacency list
        let mut adj: HashMap<String, Vec<(String, String)>> = HashMap::new();
        for edge in &graph.edges {
            let from = edge.from.to_string();
            let to = edge.to.to_string();
            adj.entry(from)
                .or_default()
                .push((to, edge.edge_type.clone()));
        }

        // Build node lookup
        let node_lookup: HashMap<String, &Node> =
            graph.nodes.iter().map(|n| (n.id.to_string(), n)).collect();

        // Find root nodes (nodes with no incoming edges)
        let has_incoming: HashSet<String> = graph.edges.iter().map(|e| e.to.to_string()).collect();

        let roots: Vec<&Node> = graph
            .nodes
            .iter()
            .filter(|n| !has_incoming.contains(&n.id.to_string()))
            .take(10)
            .collect();

        // Render each tree from root
        let mut visited: HashSet<String> = HashSet::new();

        for root in roots {
            self.render_subtree(root, &adj, &node_lookup, &mut visited, 0);
        }

        // Render any unvisited nodes
        let unvisited: Vec<&Node> = graph
            .nodes
            .iter()
            .filter(|n| !visited.contains(&n.id.to_string()))
            .take(20)
            .collect();

        if !unvisited.is_empty() {
            println!();
            println!("{}", "Other nodes:".bright_black());
            for node in unvisited {
                self.render_node(node, 1);
            }
        }

        // Summary
        println!();
        println!(
            "{} {} nodes, {} edges",
            "Summary:".bright_black(),
            graph.nodes.len(),
            graph.edges.len()
        );
        println!();

        Ok(())
    }

    /// Render a subtree recursively
    fn render_subtree(
        &self,
        node: &Node,
        adj: &HashMap<String, Vec<(String, String)>>,
        lookup: &HashMap<String, &Node>,
        visited: &mut HashSet<String>,
        depth: u32,
    ) {
        let id = node.id.to_string();

        if visited.contains(&id) || depth > self.max_depth || visited.len() >= self.max_nodes {
            return;
        }
        visited.insert(id.clone());

        // Render this node
        self.render_node(node, depth);

        // Render children
        if let Some(children) = adj.get(&id) {
            for (i, (child_id, edge_type)) in children.iter().enumerate() {
                let is_last = i == children.len() - 1;
                let prefix = "  ".repeat(depth as usize);
                let branch = if is_last { "└──" } else { "├──" };

                println!(
                    "{}{}[{}]→",
                    prefix,
                    branch.bright_black(),
                    edge_type.bright_magenta()
                );

                if let Some(child_node) = lookup.get(child_id) {
                    self.render_subtree(child_node, adj, lookup, visited, depth + 1);
                }
            }
        }
    }

    /// Render a single node
    fn render_node(&self, node: &Node, depth: u32) {
        let indent = "  ".repeat(depth as usize);

        // Get display name
        let name = node
            .properties
            .get("name")
            .map(|v| format!("{}", v))
            .unwrap_or_else(|| node.id.to_string()[..8].to_string());

        println!(
            "{}● {} {}",
            indent,
            format!("[{}]", node.node_type).bright_cyan(),
            name.bright_white()
        );

        // Show a few properties
        for (key, value) in node.properties.iter().take(3) {
            if key != "name" {
                println!(
                    "{}  {}: {}",
                    indent,
                    key.bright_black(),
                    format!("{}", value).bright_black()
                );
            }
        }
    }

    /// Render graph as DOT format (for Graphviz)
    pub fn render_dot(&self, graph: &GraphView) -> String {
        let mut lines = vec![
            "digraph G {".to_string(),
            "  rankdir=LR;".to_string(),
            "  node [shape=box, style=rounded];".to_string(),
            String::new(),
        ];

        // Define nodes
        for node in &graph.nodes {
            let id = node.id.to_string().replace('-', "_");
            let label = node
                .properties
                .get("name")
                .map(|v| format!("{}", v))
                .unwrap_or_else(|| node.node_type.clone());

            lines.push(format!(
                "  {} [label=\"{}\\n({})\"];",
                id, label, node.node_type
            ));
        }

        lines.push("".to_string());

        // Define edges
        for edge in &graph.edges {
            let from = edge.from.to_string().replace('-', "_");
            let to = edge.to.to_string().replace('-', "_");

            lines.push(format!(
                "  {} -> {} [label=\"{}\"];",
                from, to, edge.edge_type
            ));
        }

        lines.push("}".to_string());
        lines.join("\n")
    }

    /// Render graph as Mermaid format
    pub fn render_mermaid(&self, graph: &GraphView) -> String {
        let mut lines = Vec::new();
        lines.push("graph LR".to_string());

        // Define nodes with labels
        for node in &graph.nodes {
            let id = node.id.to_string()[..8].to_string();
            let label = node
                .properties
                .get("name")
                .map(|v| format!("{}", v))
                .unwrap_or_else(|| node.node_type.clone());

            lines.push(format!("  {}[{}<br/>{}]", id, label, node.node_type));
        }

        // Define edges
        for edge in &graph.edges {
            let from = edge.from.to_string()[..8].to_string();
            let to = edge.to.to_string()[..8].to_string();

            lines.push(format!("  {} -->|{}| {}", from, edge.edge_type, to));
        }

        lines.join("\n")
    }
}

impl Default for GraphRenderer {
    fn default() -> Self {
        Self::new()
    }
}
